package ext

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func (t *authzTestStore) Get(ctx Context, name string, opts *metav1.GetOptions) (*TestType, error) {
	if name == "not-found" {
		return nil, apierrors.NewNotFound(ctx.GroupVersionResource.GroupResource(), name)
	}
	return t.testStore.Get(ctx, name, opts)
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
		if err != nil || decision != authorizer.DecisionAllow {
			if err == nil {
				err = fmt.Errorf("not allowed")
			}
			forbidden := apierrors.NewForbidden(ctx.GroupVersionResource.GroupResource(), "Forbidden", err)
			forbidden.ErrStatus.Kind = "Status"
			forbidden.ErrStatus.APIVersion = "v1"
			return nil, forbidden
		}
	}
	return &testTypeListFixture, nil
}

func (s *ExtensionAPIServerSuite) TestAuthorization() {
	t := s.T()

	scheme := runtime.NewScheme()
	AddToScheme(scheme)
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
	extensionAPIServer, cleanup, err := setupExtensionAPIServer(t, scheme, &TestType{}, &TestTypeList{}, store, func(opts *ExtensionAPIServerOptions) {
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
	require.NoError(t, err)
	defer cleanup()

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
		name          string
		user          *user.DefaultInfo
		createRequest func() *http.Request

		expectedStatusCode int
		expectedStatus     apierrors.APIStatus
	}{
		{
			name: "authorized get read-only not found",
			user: &user.DefaultInfo{
				Name: "read-only",
			},
			createRequest: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/apis/ext.cattle.io/v1/testtypes/not-found", nil)
			},
			expectedStatusCode: http.StatusNotFound,
		},
		{
			name: "authorized get read-only",
			user: &user.DefaultInfo{
				Name: "read-only",
			},
			createRequest: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/apis/ext.cattle.io/v1/testtypes/foo", nil)
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "authorized list read-only",
			user: &user.DefaultInfo{
				Name: "read-only",
			},
			createRequest: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/apis/ext.cattle.io/v1/testtypes", nil)
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "unauthorized create from read-only",
			user: &user.DefaultInfo{
				Name: "read-only",
			},
			createRequest: func() *http.Request {
				return httptest.NewRequest(http.MethodPost, "/apis/ext.cattle.io/v1/testtypes", nil)
			},
			expectedStatusCode: http.StatusForbidden,
		},
		{
			name: "unauthorized update from read-only",
			user: &user.DefaultInfo{
				Name: "read-only",
			},
			createRequest: func() *http.Request {
				return httptest.NewRequest(http.MethodPut, "/apis/ext.cattle.io/v1/testtypes/foo", nil)
			},
			expectedStatusCode: http.StatusForbidden,
		},
		{
			name: "unauthorized delete from read-only",
			user: &user.DefaultInfo{
				Name: "read-only",
			},
			createRequest: func() *http.Request {
				return httptest.NewRequest(http.MethodDelete, "/apis/ext.cattle.io/v1/testtypes/foo", nil)
			},
			expectedStatusCode: http.StatusForbidden,
		},
		{
			name: "unauthorized create-on-update",
			user: &user.DefaultInfo{
				Name: "update-not-create",
			},
			createRequest: func() *http.Request {
				var buf bytes.Buffer
				json.NewEncoder(&buf).Encode(&TestType{
					TypeMeta: metav1.TypeMeta{
						Kind:       "TestType",
						APIVersion: testTypeGV.String(),
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "not-found",
					},
				})
				return httptest.NewRequest(http.MethodPut, "/apis/ext.cattle.io/v1/testtypes/not-found", &buf)
			},
			expectedStatusCode: http.StatusForbidden,
		},
		{
			name: "authorized read-only-error with custom store authorization",
			user: &user.DefaultInfo{
				Name: "read-only-error",
			},
			createRequest: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/apis/ext.cattle.io/v1/testtypes", nil)
			},
			expectedStatusCode: http.StatusForbidden,
		},
		{
			name: "authorized get read-write not found",
			user: &user.DefaultInfo{
				Name: "read-write",
			},
			createRequest: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/apis/ext.cattle.io/v1/testtypes/not-found", nil)
			},
			expectedStatusCode: http.StatusNotFound,
		},
		{
			name: "authorized get read-write",
			user: &user.DefaultInfo{
				Name: "read-write",
			},
			createRequest: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/apis/ext.cattle.io/v1/testtypes/foo", nil)
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "authorized list read-write",
			user: &user.DefaultInfo{
				Name: "read-write",
			},
			createRequest: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/apis/ext.cattle.io/v1/testtypes", nil)
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "authorized create from read-write",
			user: &user.DefaultInfo{
				Name: "read-write",
			},
			createRequest: func() *http.Request {
				var buf bytes.Buffer
				json.NewEncoder(&buf).Encode(&TestType{
					TypeMeta: metav1.TypeMeta{
						Kind:       "TestType",
						APIVersion: testTypeGV.String(),
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "foo",
					},
				})
				return httptest.NewRequest(http.MethodPost, "/apis/ext.cattle.io/v1/testtypes", &buf)
			},
			expectedStatusCode: http.StatusCreated,
		},
		{
			name: "authorized update from read-write",
			user: &user.DefaultInfo{
				Name: "read-write",
			},
			createRequest: func() *http.Request {
				var buf bytes.Buffer
				json.NewEncoder(&buf).Encode(&TestType{
					TypeMeta: metav1.TypeMeta{
						Kind:       "TestType",
						APIVersion: testTypeGV.String(),
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "foo",
					},
				})
				return httptest.NewRequest(http.MethodPut, "/apis/ext.cattle.io/v1/testtypes/foo", &buf)
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "unauthorized user",
			user: &user.DefaultInfo{
				Name: "unknown-user",
			},
			createRequest: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/apis/ext.cattle.io/v1/testtypes", nil)
			},
			expectedStatusCode: http.StatusForbidden,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := test.createRequest()
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
				require.Equal(t, test.expectedStatus.Status(), responseStatus, "for request "+req.URL.String())
			}
		})
	}
}
