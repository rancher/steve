package ext

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rancher/lasso/pkg/controller"
	"github.com/rancher/steve/pkg/accesscontrol"
	wrbacv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/rbac/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/request"
)

type authzTestStore struct {
	*testStore
}

func (t *authzTestStore) List(ctx Context, opts *metav1.ListOptions) (*TestTypeList, error) {
	if ctx.User.GetName() == "read-only-error" {
		decision, _, err := ctx.Authorizer.Authorize(ctx, authorizer.AttributesRecord{
			User:            ctx.User,
			Verb:            "customverb",
			Resource:        "testtypes",
			ResourceRequest: true,
			APIGroup:        "ext.cattle.io",
		})
		if decision != authorizer.DecisionAllow {
			forbidden := apierrors.NewForbidden(schema.GroupResource{
				Group:    "ext.cattle.io",
				Resource: "testtypes",
			}, "Forbidden", err)
			forbidden.ErrStatus.Kind = "Status"
			forbidden.ErrStatus.APIVersion = "v1"
			return nil, forbidden
		}
	}
	return &testTypeListFixture, nil
}

func (s *ExtensionAPIServerSuite) TestAuthorization() {
	t := s.T()

	rbacv1.AddToScheme(scheme)
	codecs := serializer.NewCodecFactory(scheme)

	controllerFactory, err := controller.NewSharedControllerFactoryFromConfigWithOptions(s.restConfig, scheme, &controller.SharedControllerFactoryOptions{})
	require.NoError(t, err)

	rbacController := wrbacv1.New(controllerFactory)

	accessStore := accesscontrol.NewAccessStore(s.ctx, false, rbacController)
	authz := NewAccessSetAuthorizer(accessStore)

	err = controllerFactory.Start(s.ctx, 2)
	require.NoError(t, err)

	store := &authzTestStore{
		testStore: &testStore{},
	}
	extensionAPIServer, cleanup, err := setupExtensionAPIServer(t, store, func(opts *ExtensionAPIServerOptions) {
		// XXX: Find a way to get rid of this
		opts.BindPort = 32000
		opts.Client = s.client
		opts.Authorization = authz
		opts.Authentication = AuthenticationOptions{
			CustomAuthenticator: authenticator.RequestFunc(func(req *http.Request) (*authenticator.Response, bool, error) {
				user, ok := request.UserFrom(req.Context())
				if !ok {
					return nil, false, nil
				}
				return &authenticator.Response{
					User: user,
				}, true, nil
			}),
		}
	})
	defer cleanup()
	require.NoError(t, err)

	rbacBytes, err := os.ReadFile(filepath.Join("testdata", "rbac.yaml"))
	require.NoError(t, err)

	decoder := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(rbacBytes), 4096)
	for {
		var rawObj runtime.RawExtension
		if err = decoder.Decode(&rawObj); err != nil {
			break
		}

		obj, _, err := codecs.UniversalDecoder(rbacv1.SchemeGroupVersion).Decode(rawObj.Raw, nil, nil)
		require.NoError(t, err)

		switch obj := obj.(type) {
		case *rbacv1.ClusterRole:
			_, err = s.client.RbacV1().ClusterRoles().Create(s.ctx, obj, metav1.CreateOptions{})
			defer func(name string) {
				s.client.RbacV1().ClusterRoles().Delete(s.ctx, obj.GetName(), metav1.DeleteOptions{})
			}(obj.GetName())
		case *rbacv1.ClusterRoleBinding:
			_, err = s.client.RbacV1().ClusterRoleBindings().Create(s.ctx, obj, metav1.CreateOptions{})
			defer func(name string) {
				s.client.RbacV1().ClusterRoleBindings().Delete(s.ctx, obj.GetName(), metav1.DeleteOptions{})
			}(obj.GetName())
		}
		require.NoError(t, err, "creating")
	}

	tests := []struct {
		name  string
		user  *user.DefaultInfo
		paths []string

		expectedStatusCode int
		expectedStatus     apierrors.APIStatus
	}{
		{
			name: "authorized read-only",
			user: &user.DefaultInfo{
				Name: "read-only",
			},
			paths:              []string{"/apis/ext.cattle.io/v1/testtypes"},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "authorized read-only-error with custom store authorization",
			user: &user.DefaultInfo{
				Name: "read-only-error",
			},
			paths:              []string{"/apis/ext.cattle.io/v1/testtypes"},
			expectedStatusCode: http.StatusForbidden,
		},
		{
			name: "unauthorized user",
			user: &user.DefaultInfo{
				Name: "unknown-user",
			},
			paths:              []string{"/apis/ext.cattle.io/v1/testtypes"},
			expectedStatusCode: http.StatusForbidden,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, path := range test.paths {
				req := httptest.NewRequest(http.MethodGet, path, nil)
				w := httptest.NewRecorder()

				if test.user != nil {
					assert.EventuallyWithT(t, func(c *assert.CollectT) {
						assert.True(c, authz.hasUser(test.user.GetName()))
					}, time.Second*5, 100*time.Millisecond)

					ctx := request.WithUser(req.Context(), test.user)
					req = req.WithContext(ctx)
				}

				extensionAPIServer.ServeHTTP(w, req)
				resp := w.Result()

				body, _ := io.ReadAll(resp.Body)
				responseStatus := metav1.Status{}
				json.Unmarshal(body, &responseStatus)

				require.Equal(t, test.expectedStatusCode, resp.StatusCode)
				if test.expectedStatus != nil {
					require.Equal(t, test.expectedStatus.Status(), responseStatus, "for path "+path)
				}
			}
		})
	}
}
