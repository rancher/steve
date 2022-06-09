package schema

import (
	"fmt"
	"net/http"
	"time"

	"github.com/rancher/apiserver/pkg/builtin"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/attributes"
	v1 "github.com/rancher/wrangler/pkg/generated/controllers/rbac/v1"
	"github.com/sirupsen/logrus"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
)

func newSchemas() (*types.APISchemas, error) {
	apiSchemas := types.EmptyAPISchemas()
	if err := apiSchemas.AddSchemas(builtin.Schemas); err != nil {
		return nil, err
	}

	return apiSchemas, nil
}

func (c *Collection) Schemas(user user.Info) (*types.APISchemas, error) {
	access := c.as.AccessFor(user)
	val, ok := c.cache.Get(access.ID)
	if ok {
		schemas, _ := val.(*types.APISchemas)
		return schemas, nil
	}

	schemas, err := c.schemasForSubject(access)
	if err != nil {
		return nil, err
	}

	c.cache.Add(access.ID, schemas, 24*time.Hour)
	return schemas, nil
}

func (c *Collection) setup(rbac v1.Interface) error {
	c.watchLock.Lock()
	defer c.watchLock.Unlock()
	if c.roleBindings == nil {
		c.roleBindings = rbac.RoleBinding()
	}
	if c.rbByUser == nil {
		c.rbByUser = make(map[string]chan watch.Event)
	}
	if c.rbWatch == nil {
		rbs, err := c.roleBindings.List("", metav1.ListOptions{})
		if err != nil {
			return err
		}
		logrus.Tracef("[steve.schema.Collection] starting rolebindings watch")
		timeout := int64(30 * 60)
		c.rbWatch, err = c.roleBindings.Watch("", metav1.ListOptions{
			ResourceVersion: rbs.ResourceVersion,
			TimeoutSeconds:  &timeout,
		})
		if err != nil {
			return err
		}
		go func() {
			defer func() { c.rbWatch = nil }()
			defer c.rbWatch.Stop()
			for e := range c.rbWatch.ResultChan() {
				rb := e.Object.(*rbacv1.RoleBinding)
				for _, s := range rb.Subjects {
					logrus.Tracef("[steve.schema.Collection] got event for user [%s] : %+v", s.Name, e)
					if _, ok := c.rbByUser[s.Name]; !ok {
						c.rbByUser[s.Name] = make(chan watch.Event, 10)
					}
					c.rbByUser[s.Name] <- e
				}
			}
			logrus.Tracef("[steve.schema.Collection] role binding watch closed")
		}()
	}
	if c.watchesStarted == nil {
		c.watchesStarted = make(map[string]struct{})
	}
	return nil
}

func (c *Collection) SchemasWithWatch(user user.Info, rbac v1.Interface) (*types.APISchemas, error) {
	if err := c.setup(rbac); err != nil {
		return nil, err
	}
	access := c.as.AccessFor(user)
	if _, ok := c.watchesStarted[user.GetUID()]; !ok {
		go func() {
			c.watchesStarted[user.GetUID()] = struct{}{}
			defer delete(c.watchesStarted, user.GetUID())
			if _, ok := c.rbByUser[user.GetUID()]; !ok {
				c.rbByUser[user.GetUID()] = make(chan watch.Event, 10) // FIXME handle groups
			}
			for e := range c.rbByUser[user.GetUID()] {
				rb := e.Object.(*rbacv1.RoleBinding)
				switch e.Type {
				case watch.Added:
					c.as.AddAccess(access, rb.Namespace, rb.RoleRef)
				case watch.Modified:
					userInSubjects := false
					for _, s := range rb.Subjects {
						if s.Name == user.GetUID() {
							userInSubjects = true
							break
						}
					}
					if userInSubjects {
						c.as.AddAccess(access, rb.Namespace, rb.RoleRef)
					} else {
						c.as.RemoveAccess(access, rb.Namespace, rb.RoleRef)
					}
				case watch.Deleted:
					c.as.RemoveAccess(access, rb.Namespace, rb.RoleRef)
				default:
					continue
				}
				schemas, err := c.schemasForSubject(access)
				if err != nil {
					logrus.Errorf("steve schemas error: %v", err) // FIXME real error handling
				}
				c.lock.Lock()
				c.cache.Add(access.ID, schemas, 24*time.Hour)
				c.lock.Unlock()
			}
			logrus.Tracef("[steve.schema.Collection] role binding by user channel closed")
		}()
	}

	c.lock.RLock()
	val, ok := c.cache.Get(access.ID)
	c.lock.RUnlock()
	if ok {
		schemas, _ := val.(*types.APISchemas)
		return schemas, nil
	}

	schemas, err := c.schemasForSubject(access)
	if err != nil {
		return nil, err
	}

	c.lock.Lock()
	c.cache.Add(access.ID, schemas, 24*time.Hour)
	c.lock.Unlock()
	return schemas, nil
}

func (c *Collection) schemasForSubject(access *accesscontrol.AccessSet) (*types.APISchemas, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	result, err := newSchemas()
	if err != nil {
		return nil, err
	}

	if err := result.AddSchemas(c.baseSchema); err != nil {
		return nil, err
	}

	for _, s := range c.schemas {
		gr := attributes.GR(s)

		if gr.Resource == "" {
			if err := result.AddSchema(*s); err != nil {
				return nil, err
			}
			continue
		}

		verbs := attributes.Verbs(s)
		verbAccess := accesscontrol.AccessListByVerb{}

		for _, verb := range verbs {
			a := access.AccessListFor(verb, gr)
			if !attributes.Namespaced(s) {
				// trim out bad data where we are granted namespaced access to cluster scoped object
				result := accesscontrol.AccessList{}
				for _, access := range a {
					if access.Namespace == accesscontrol.All {
						result = append(result, access)
					}
				}
				a = result
			}
			if len(a) > 0 {
				verbAccess[verb] = a
			}
		}

		if len(verbAccess) == 0 {
			if gr.Group == "" && gr.Resource == "namespaces" {
				var accessList accesscontrol.AccessList
				for _, ns := range access.Namespaces() {
					accessList = append(accessList, accesscontrol.Access{
						Namespace:    accesscontrol.All,
						ResourceName: ns,
					})
				}
				verbAccess["get"] = accessList
				verbAccess["watch"] = accessList
				if len(accessList) == 0 {
					// always allow list
					s.CollectionMethods = append(s.CollectionMethods, http.MethodGet)
				}
			}
		}

		allowed := func(method string) string {
			if attributes.DisallowMethods(s)[method] {
				return "blocked-" + method
			}
			return method
		}

		s = s.DeepCopy()
		attributes.SetAccess(s, verbAccess)
		if verbAccess.AnyVerb("list", "get") {
			s.ResourceMethods = append(s.ResourceMethods, allowed(http.MethodGet))
			s.CollectionMethods = append(s.CollectionMethods, allowed(http.MethodGet))
		}
		if verbAccess.AnyVerb("delete") {
			s.ResourceMethods = append(s.ResourceMethods, allowed(http.MethodDelete))
		}
		if verbAccess.AnyVerb("update") {
			s.ResourceMethods = append(s.ResourceMethods, allowed(http.MethodPut))
			s.ResourceMethods = append(s.ResourceMethods, allowed(http.MethodPatch))
		}
		if verbAccess.AnyVerb("create") {
			s.CollectionMethods = append(s.CollectionMethods, allowed(http.MethodPost))
		}

		if len(s.CollectionMethods) == 0 && len(s.ResourceMethods) == 0 {
			continue
		}

		if err := result.AddSchema(*s); err != nil {
			return nil, err
		}
	}

	result.Attributes = map[string]interface{}{
		"accessSet": access,
	}
	return result, nil
}

func (c *Collection) defaultStore() types.Store {
	templates := c.templates[""]
	if len(templates) > 0 {
		return templates[0].Store
	}
	return nil
}

func (c *Collection) applyTemplates(schema *types.APISchema) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	templates := [][]*Template{
		c.templates[schema.ID],
		c.templates[fmt.Sprintf("%s/%s", attributes.Group(schema), attributes.Kind(schema))],
		c.templates[""],
	}

	for _, templates := range templates {
		for _, t := range templates {
			if t == nil {
				continue
			}
			if schema.Formatter == nil {
				schema.Formatter = t.Formatter
			} else if t.Formatter != nil {
				schema.Formatter = types.FormatterChain(t.Formatter, schema.Formatter)
			}
			if schema.Store == nil {
				if t.StoreFactory == nil {
					schema.Store = t.Store
				} else {
					schema.Store = t.StoreFactory(c.defaultStore())
				}
			}
			if t.Customize != nil {
				t.Customize(schema)
			}
		}
	}
}
