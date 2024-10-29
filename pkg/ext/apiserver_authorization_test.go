package ext

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rancher/lasso/pkg/controller"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/accesscontrol/fake"
	wrbacv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/rbac/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
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
	"k8s.io/apiserver/pkg/server/options"
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

	ln, _, err := options.CreateListener("", ":0", net.ListenConfig{})
	require.NoError(t, err)

	store := &authzTestStore{
		testStore: &testStore{},
	}
	extensionAPIServer, cleanup, err := setupExtensionAPIServer(t, scheme, &TestType{}, &TestTypeList{}, store, func(opts *ExtensionAPIServerOptions) {
		opts.Listener = ln
		opts.Authorizer = authz
		opts.Authenticator = authenticator.RequestFunc(func(req *http.Request) (*authenticator.Response, bool, error) {
			user, ok := request.UserFrom(req.Context())
			if !ok {
				return nil, false, nil
			}
			return &authenticator.Response{
				User: user,
			}, true, nil
		})
	}, nil)
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
		{
			name: "authorized access to non-resource url",
			user: &user.DefaultInfo{
				Name: "openapi-v2-only",
			},
			createRequest: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/openapi/v2", nil)
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "unauthorized verb to non-resource url",
			user: &user.DefaultInfo{
				Name: "openapi-v2-only",
			},
			createRequest: func() *http.Request {
				return httptest.NewRequest(http.MethodPost, "/openapi/v2", nil)
			},
			expectedStatusCode: http.StatusForbidden,
		},
		{
			name: "unauthorized access to non-resource url (user can access only openapi/v2)",
			user: &user.DefaultInfo{
				Name: "openapi-v2-only",
			},
			createRequest: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/openapi/v3", nil)
			},
			expectedStatusCode: http.StatusForbidden,
		},
		{
			name: "authorized user can access both openapi v2 and v3 (v2)",
			user: &user.DefaultInfo{
				Name: "openapi-v2-v3",
			},
			createRequest: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/openapi/v2", nil)
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "authorized user can access both openapi v2 and v3 (v3)",
			user: &user.DefaultInfo{
				Name: "openapi-v2-v3",
			},
			createRequest: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/openapi/v3", nil)
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "authorized user can access url based in wildcard rule",
			user: &user.DefaultInfo{
				Name: "openapi-v2-v3",
			},
			createRequest: func() *http.Request {
				return httptest.NewRequest(http.MethodGet, "/openapi/v3/apis/ext.cattle.io/v1", nil)
			},
			expectedStatusCode: http.StatusOK,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := test.createRequest()
			w := httptest.NewRecorder()

			if test.user != nil {
				assert.EventuallyWithT(t, func(c *assert.CollectT) {
					accessSet := accessStore.AccessFor(test.user)
					assert.NotNil(c, accessSet)
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

func TestAuthorization_NonResourceURLs(t *testing.T) {
	type input struct {
		ctx   context.Context
		attrs authorizer.Attributes
	}

	type expected struct {
		authorized authorizer.Decision
		reason     string
		err        error
	}

	sampleReadOnlyUser := &user.DefaultInfo{
		Name: "read-only-user",
	}

	sampleReadOnlyAccessSet := func() *accesscontrol.AccessSet {
		accessSet := &accesscontrol.AccessSet{}
		accessSet.AddNonResourceURLs([]string{
			"get",
		}, []string{
			"/metrics",
			"/healthz",
		})
		return accessSet
	}()

	sampleReadWriteUser := &user.DefaultInfo{
		Name: "read-write-user",
	}

	sampleReadWriteAccessSet := func() *accesscontrol.AccessSet {
		accessSet := &accesscontrol.AccessSet{}
		accessSet.AddNonResourceURLs([]string{
			"get", "post",
		}, []string{
			"/metrics",
			"/healthz",
		})
		return accessSet
	}()

	tests := []struct {
		name     string
		input    input
		expected expected

		mockUsername  *user.DefaultInfo
		mockAccessSet *accesscontrol.AccessSet
	}{
		{
			name: "authorized read-only user to read data",
			input: input{
				ctx: context.TODO(),
				attrs: authorizer.AttributesRecord{
					User:            sampleReadOnlyUser,
					ResourceRequest: false,
					Path:            "/healthz",
					Verb:            "get",
				},
			},
			expected: expected{
				authorized: authorizer.DecisionAllow,
				reason:     "",
				err:        nil,
			},
			mockUsername:  sampleReadOnlyUser,
			mockAccessSet: sampleReadOnlyAccessSet,
		},
		{
			name: "unauthorized read-only user to write data",
			input: input{
				ctx: context.TODO(),
				attrs: authorizer.AttributesRecord{
					User:            sampleReadOnlyUser,
					ResourceRequest: false,
					Path:            "/metrics",
					Verb:            "post",
				},
			},
			expected: expected{
				authorized: authorizer.DecisionDeny,
				reason:     "",
				err:        nil,
			},
			mockUsername:  sampleReadOnlyUser,
			mockAccessSet: sampleReadOnlyAccessSet,
		},
		{
			name: "authorized read-write user to read data",
			input: input{
				ctx: context.TODO(),
				attrs: authorizer.AttributesRecord{
					User:            sampleReadWriteUser,
					ResourceRequest: false,
					Path:            "/metrics",
					Verb:            "get",
				},
			},
			expected: expected{
				authorized: authorizer.DecisionAllow,
				reason:     "",
				err:        nil,
			},
			mockUsername:  sampleReadWriteUser,
			mockAccessSet: sampleReadWriteAccessSet,
		},
		{
			name: "authorized read-write user to write data",
			input: input{
				ctx: context.TODO(),
				attrs: authorizer.AttributesRecord{
					User:            sampleReadWriteUser,
					ResourceRequest: false,
					Path:            "/metrics",
					Verb:            "post",
				},
			},
			expected: expected{
				authorized: authorizer.DecisionAllow,
				reason:     "",
				err:        nil,
			},
			mockUsername:  sampleReadWriteUser,
			mockAccessSet: sampleReadWriteAccessSet,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			crtl := gomock.NewController(t)
			asl := fake.NewMockAccessSetLookup(crtl)
			asl.EXPECT().AccessFor(tt.mockUsername).Return(tt.mockAccessSet)

			auth := NewAccessSetAuthorizer(asl)
			authorized, reason, err := auth.Authorize(tt.input.ctx, tt.input.attrs)

			require.Equal(t, tt.expected.authorized, authorized)
			require.Equal(t, tt.expected.reason, reason)

			if tt.expected.err != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
