package ext

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server/options"
)

type authnTestStore struct {
	*testStore
	userCh chan user.Info
}

func (t *authnTestStore) List(ctx Context, opts *metav1.ListOptions) (*TestTypeList, error) {
	t.userCh <- ctx.User
	return &testTypeListFixture, nil
}

func (t *authnTestStore) getUser() (user.Info, bool) {
	timer := time.NewTimer(time.Second * 5)
	defer timer.Stop()
	select {
	case user := <-t.userCh:
		return user, true
	case <-timer.C:
		return nil, false
	}
}

func (s *ExtensionAPIServerSuite) TestAuthenticationCustom() {
	t := s.T()

	scheme := runtime.NewScheme()
	AddToScheme(scheme)

	ln, _, err := options.CreateListener("", ":0", net.ListenConfig{})
	require.NoError(t, err)

	store := &authnTestStore{
		testStore: &testStore{},
		userCh:    make(chan user.Info, 100),
	}
	extensionAPIServer, cleanup, err := setupExtensionAPIServer(t, scheme, &TestType{}, &TestTypeList{}, store, func(opts *ExtensionAPIServerOptions) {
		opts.Listener = ln
		opts.Authorizer = authorizer.AuthorizerFunc(authzAllowAll)
		opts.Authenticator = authenticator.RequestFunc(func(req *http.Request) (*authenticator.Response, bool, error) {
			user, ok := request.UserFrom(req.Context())
			if !ok {
				return nil, false, nil
			}
			if user.GetName() == "error" {
				return nil, false, fmt.Errorf("fake error")
			}
			return &authenticator.Response{
				User: user,
			}, true, nil
		})
	}, nil)
	require.NoError(t, err)
	defer cleanup()

	unauthorized := apierrors.NewUnauthorized("Unauthorized")
	unauthorized.ErrStatus.Kind = "Status"
	unauthorized.ErrStatus.APIVersion = "v1"

	allPaths := []string{
		"/",
		"/apis",
		"/apis/ext.cattle.io",
		"/apis/ext.cattle.io/v1",
		"/apis/ext.cattle.io/v1/testtypes",
		"/apis/ext.cattle.io/v1/testtypes/foo",
		"/openapi/v2",
		"/openapi/v3",
		"/openapi/v3/apis/ext.cattle.io/v1",
	}

	tests := []struct {
		name  string
		user  *user.DefaultInfo
		paths []string

		expectedStatusCode int
		expectedStatus     apierrors.APIStatus
		expectedUser       *user.DefaultInfo
	}{
		{
			name:  "authenticated request check user",
			paths: []string{"/apis/ext.cattle.io/v1/testtypes"},
			user:  &user.DefaultInfo{Name: "my-user", Groups: []string{"my-group", "system:authenticated"}, Extra: map[string][]string{}},

			expectedStatusCode: http.StatusOK,
			expectedUser:       &user.DefaultInfo{Name: "my-user", Groups: []string{"my-group", "system:authenticated"}, Extra: map[string][]string{}},
		},
		{
			name:  "authenticated request all paths",
			user:  &user.DefaultInfo{Name: "my-user", Groups: []string{"my-group", "system:authenticated"}, Extra: map[string][]string{}},
			paths: allPaths,

			expectedStatusCode: http.StatusOK,
		},
		{
			name:  "authenticated request to unknown endpoint",
			user:  &user.DefaultInfo{Name: "my-user", Groups: []string{"my-group", "system:authenticated"}, Extra: map[string][]string{}},
			paths: []string{"/unknown"},

			expectedStatusCode: http.StatusNotFound,
		},
		{
			name:  "unauthenticated request",
			paths: append(allPaths, "/unknown"),

			expectedStatusCode: http.StatusUnauthorized,
			expectedStatus:     unauthorized,
		},
		{
			name:  "authentication error",
			user:  &user.DefaultInfo{Name: "error"},
			paths: append(allPaths, "/unknown"),

			expectedStatusCode: http.StatusUnauthorized,
			expectedStatus:     unauthorized,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, path := range test.paths {
				req := httptest.NewRequest(http.MethodGet, path, nil)
				w := httptest.NewRecorder()

				if test.user != nil {
					ctx := request.WithUser(req.Context(), test.user)
					req = req.WithContext(ctx)
				}

				extensionAPIServer.ServeHTTP(w, req)
				resp := w.Result()

				body, _ := io.ReadAll(resp.Body)

				responseStatus := metav1.Status{}
				json.Unmarshal(body, &responseStatus)

				require.Equal(t, test.expectedStatusCode, resp.StatusCode, "for path "+path)
				if test.expectedStatus != nil {
					require.Equal(t, test.expectedStatus.Status(), responseStatus, "for path "+path)
				}
				if test.expectedUser != nil {
					authUser, found := store.getUser()
					require.True(t, found)
					require.Equal(t, test.expectedUser, authUser)
				}
			}
		})
	}
}
