package ext

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/request"
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

func (s *ExtensionAPIServerSuite) TestAuthenticationBuiltin() {
	t := s.T()

	// Same CA but CN not in the list allowed
	notAllowedCertPair, err := s.ca.NewClientCert("system:not-allowed")
	require.NoError(t, err)
	notAllowedCert, notAllowedKey, err := notAllowedCertPair.AsBytes()
	require.NoError(t, err)

	badCA, err := NewTinyCA()
	require.NoError(t, err)
	badCertPair, err := badCA.NewClientCert("system:auth-proxy")
	require.NoError(t, err)
	badCert, badKey, err := badCertPair.AsBytes()
	require.NoError(t, err)

	cert, key, err := s.cert.AsBytes()
	require.NoError(t, err)
	certificate, err := tls.X509KeyPair(cert, key)
	require.NoError(t, err)

	badCACertificate, err := tls.X509KeyPair(badCert, badKey)
	require.NoError(t, err)

	notAllowedCertificate, err := tls.X509KeyPair(notAllowedCert, notAllowedKey)
	require.NoError(t, err)

	scheme := runtime.NewScheme()
	AddToScheme(scheme)

	store := &authnTestStore{
		testStore: &testStore{},
		userCh:    make(chan user.Info, 100),
	}
	builtinAuth, err := NewBuiltinAuthenticator(s.client)
	require.NoError(t, err)

	func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		err = builtinAuth.RunOnce(ctx)
		require.NoError(t, err)
	}()

	_, cleanup, err := setupExtensionAPIServer(t, scheme, &TestType{}, &TestTypeList{}, store, func(opts *ExtensionAPIServerOptions) {
		// XXX: Find a way to get rid of this
		opts.BindPort = 32003
		opts.Client = s.client
		opts.Authenticator = builtinAuth
		opts.Authorizer = authorizer.AuthorizerFunc(authzAllowAll)
	})
	require.NoError(t, err)
	defer cleanup()

	allPaths := []string{
		"/",
		"/openapi/v2",
		"/openapi/v3",
		"/openapi/v3/apis/ext.cattle.io/v1",
		"/apis",
		"/apis/ext.cattle.io",
		"/apis/ext.cattle.io/v1",
		"/apis/ext.cattle.io/v1/testtypes",
		"/apis/ext.cattle.io/v1/testtypes/foo",
	}

	type test struct {
		name   string
		certs  []tls.Certificate
		paths  []string
		user   string
		groups []string

		expectedStatusCode int
		expectedUser       *user.DefaultInfo
	}
	tests := []test{
		{
			name:   "authenticated request check user",
			certs:  []tls.Certificate{certificate},
			paths:  []string{"/apis/ext.cattle.io/v1/testtypes"},
			user:   "my-user",
			groups: []string{"my-group"},

			expectedStatusCode: http.StatusOK,
			expectedUser:       &user.DefaultInfo{Name: "my-user", Groups: []string{"my-group", "system:authenticated"}, Extra: map[string][]string{}},
		},
		{
			name:   "authenticated request all paths",
			certs:  []tls.Certificate{certificate},
			paths:  allPaths,
			user:   "my-user",
			groups: []string{"my-group"},

			expectedStatusCode: http.StatusOK,
		},
		{
			name:   "authenticated request to unknown endpoint",
			certs:  []tls.Certificate{certificate},
			paths:  []string{"/unknown"},
			user:   "my-user",
			groups: []string{"my-group"},

			expectedStatusCode: http.StatusNotFound,
		},
		{
			name:   "no client certs",
			paths:  append(allPaths, "/unknown"),
			user:   "my-user",
			groups: []string{"my-group"},

			expectedStatusCode: http.StatusUnauthorized,
		},
		{
			name:   "client certs from bad CA",
			certs:  []tls.Certificate{badCACertificate},
			paths:  append(allPaths, "/unknown"),
			user:   "my-user",
			groups: []string{"my-group"},

			expectedStatusCode: http.StatusUnauthorized,
		},
		{
			name:   "client certs with CN not allowed",
			certs:  []tls.Certificate{notAllowedCertificate},
			paths:  append(allPaths, "/unknown"),
			user:   "my-user",
			groups: []string{"my-group"},

			expectedStatusCode: http.StatusUnauthorized,
		},
		{
			name:   "no user",
			paths:  append(allPaths, "/unknown"),
			groups: []string{"my-group"},

			expectedStatusCode: http.StatusUnauthorized,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			httpClient := http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						InsecureSkipVerify: true,
						Certificates:       test.certs,
					},
				},
			}

			for _, path := range test.paths {
				req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("https://127.0.0.1:%d%s", 32003, path), nil)
				require.NoError(t, err)
				if test.user != "" {
					req.Header.Set("X-Remote-User", test.user)
				}
				for _, group := range test.groups {
					req.Header.Add("X-Remote-Group", group)
				}

				// Eventually because the cache for auth might not be synced yet
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					resp, err := httpClient.Do(req)
					require.NoError(t, err)
					defer resp.Body.Close()

					require.Equal(t, test.expectedStatusCode, resp.StatusCode)
				}, 5*time.Second, 110*time.Millisecond)

				if test.expectedUser != nil {
					authUser, found := store.getUser()
					require.True(t, found)
					require.Equal(t, test.expectedUser, authUser)
				}
			}
		})
	}
}

func (s *ExtensionAPIServerSuite) TestAuthenticationCustom() {
	t := s.T()

	scheme := runtime.NewScheme()
	AddToScheme(scheme)

	store := &authnTestStore{
		testStore: &testStore{},
		userCh:    make(chan user.Info, 100),
	}
	extensionAPIServer, cleanup, err := setupExtensionAPIServer(t, scheme, &TestType{}, &TestTypeList{}, store, func(opts *ExtensionAPIServerOptions) {
		// XXX: Find a way to get rid of this
		opts.BindPort = 32000
		opts.Client = s.client
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
	})
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

func (s *ExtensionAPIServerSuite) TestAuthenticationUnion() {
	t := s.T()

	scheme := runtime.NewScheme()
	AddToScheme(scheme)

	cert, key, err := s.cert.AsBytes()
	require.NoError(t, err)
	certificate, err := tls.X509KeyPair(cert, key)
	require.NoError(t, err)

	builtinAuth, err := NewBuiltinAuthenticator(s.client)
	require.NoError(t, err)

	customAuth := authenticator.RequestFunc(func(req *http.Request) (*authenticator.Response, bool, error) {
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
	auth := NewUnionAuthenticator(customAuth, builtinAuth)
	func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		err = auth.RunOnce(ctx)
		require.NoError(t, err)
	}()

	store := &authnTestStore{
		testStore: &testStore{},
		userCh:    make(chan user.Info, 100),
	}
	extensionAPIServer, cleanup, err := setupExtensionAPIServer(t, scheme, &TestType{}, &TestTypeList{}, store, func(opts *ExtensionAPIServerOptions) {
		// XXX: Find a way to get rid of this
		opts.BindPort = 32004
		opts.Client = s.client
		opts.Authorizer = authorizer.AuthorizerFunc(authzAllowAll)
		opts.Authenticator = auth
	})
	require.NoError(t, err)
	defer cleanup()

	httpClient := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
				Certificates:       []tls.Certificate{certificate},
			},
		},
	}
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("https://127.0.0.1:%d%s", 32004, "/openapi/v2"), nil)
	require.NoError(t, err)

	userInfo := &user.DefaultInfo{
		Name:   "my-user",
		Groups: []string{"my-group"},
	}

	req.Header.Set("X-Remote-User", userInfo.GetName())
	req.Header.Add("X-Remote-Group", userInfo.GetGroups()[0])
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)
	}, 5*time.Second, 110*time.Millisecond)

	req = httptest.NewRequest(http.MethodGet, "/openapi/v2", nil)
	w := httptest.NewRecorder()

	ctx := request.WithUser(req.Context(), userInfo)
	req = req.WithContext(ctx)

	extensionAPIServer.ServeHTTP(w, req)
	resp := w.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
