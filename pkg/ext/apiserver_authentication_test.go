package ext

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	crand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rancher/lasso/pkg/controller"
	"github.com/rancher/steve/pkg/accesscontrol"
	wrbacv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/rbac/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/kubernetes"
	certutil "k8s.io/client-go/util/cert"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// Copied and modified from envtest internal
var (
	ellipticCurve = elliptic.P256()
	bigOne        = big.NewInt(1)
)

// CertPair is a private key and certificate for use for client auth, as a CA, or serving.
type CertPair struct {
	Key  crypto.Signer
	Cert *x509.Certificate
}

// CertBytes returns the PEM-encoded version of the certificate for this pair.
func (k CertPair) CertBytes() []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: k.Cert.Raw,
	})
}

// AsBytes encodes keypair in the appropriate formats for on-disk storage (PEM and
// PKCS8, respectively).
func (k CertPair) AsBytes() (cert []byte, key []byte, err error) {
	cert = k.CertBytes()

	rawKeyData, err := x509.MarshalPKCS8PrivateKey(k.Key)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to encode private key: %w", err)
	}

	key = pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: rawKeyData,
	})

	return cert, key, nil
}

// TinyCA supports signing serving certs and client-certs,
// and can be used as an auth mechanism with envtest.
type TinyCA struct {
	CA      CertPair
	orgName string

	nextSerial *big.Int
}

// newPrivateKey generates a new private key of a relatively sane size (see
// rsaKeySize).
func newPrivateKey() (crypto.Signer, error) {
	return ecdsa.GenerateKey(ellipticCurve, crand.Reader)
}

// NewTinyCA creates a new a tiny CA utility for provisioning serving certs and client certs FOR TESTING ONLY.
// Don't use this for anything else!
func NewTinyCA() (*TinyCA, error) {
	caPrivateKey, err := newPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("unable to generate private key for CA: %w", err)
	}
	caCfg := certutil.Config{CommonName: "envtest-environment", Organization: []string{"envtest"}}
	caCert, err := certutil.NewSelfSignedCACert(caCfg, caPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("unable to generate certificate for CA: %w", err)
	}

	return &TinyCA{
		CA:         CertPair{Key: caPrivateKey, Cert: caCert},
		orgName:    "envtest",
		nextSerial: big.NewInt(1),
	}, nil
}

func (c *TinyCA) CertBytes() []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: c.CA.Cert.Raw,
	})
}

func (c *TinyCA) NewClientCert(name string) (CertPair, error) {
	return c.makeCert(certutil.Config{
		CommonName: name,
		Usages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
}

func (c *TinyCA) makeCert(cfg certutil.Config) (CertPair, error) {
	now := time.Now()

	key, err := newPrivateKey()
	if err != nil {
		return CertPair{}, fmt.Errorf("unable to create private key: %w", err)
	}

	serial := new(big.Int).Set(c.nextSerial)
	c.nextSerial.Add(c.nextSerial, bigOne)

	template := x509.Certificate{
		Subject:      pkix.Name{CommonName: cfg.CommonName, Organization: cfg.Organization},
		DNSNames:     cfg.AltNames.DNSNames,
		IPAddresses:  cfg.AltNames.IPs,
		SerialNumber: serial,

		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: cfg.Usages,

		// technically not necessary for testing, but let's set anyway just in case.
		NotBefore: now.UTC(),
		// 1 week -- the default for cfssl, and just long enough for a
		// long-term test, but not too long that anyone would try to use this
		// seriously.
		NotAfter: now.Add(168 * time.Hour).UTC(),
	}

	certRaw, err := x509.CreateCertificate(crand.Reader, &template, c.CA.Cert, key.Public(), c.CA.Key)
	if err != nil {
		return CertPair{}, fmt.Errorf("unable to create certificate: %w", err)
	}

	cert, err := x509.ParseCertificate(certRaw)
	if err != nil {
		return CertPair{}, fmt.Errorf("generated invalid certificate, could not parse: %w", err)
	}

	return CertPair{
		Key:  key,
		Cert: cert,
	}, nil
}

type authTestStore struct {
	*testStore
	userCh chan user.Info
}

func (t *authTestStore) List(ctx context.Context, userInfo user.Info, opts *metav1.ListOptions) (*TestTypeList, error) {
	t.userCh <- userInfo
	return &testTypeListFixture, nil
}

func (t *authTestStore) getUser() (user.Info, bool) {
	timer := time.NewTimer(time.Second * 5)
	defer timer.Stop()
	select {
	case user := <-t.userCh:
		return user, true
	case <-timer.C:
		return nil, false
	}
}

func TestAuthentication(t *testing.T) {
	tinyCA, err := NewTinyCA()
	require.NoError(t, err)
	certPair, err := tinyCA.NewClientCert("system:auth-proxy")
	require.NoError(t, err)
	cert, key, err := certPair.AsBytes()
	require.NoError(t, err)

	notAllowedCertPair, err := tinyCA.NewClientCert("system:not-allowed")
	require.NoError(t, err)
	notAllowedCert, notAllowedKey, err := notAllowedCertPair.AsBytes()
	require.NoError(t, err)

	badCA, err := NewTinyCA()
	require.NoError(t, err)
	badCertPair, err := badCA.NewClientCert("system:auth-proxy")
	require.NoError(t, err)
	badCert, badKey, err := badCertPair.AsBytes()
	require.NoError(t, err)

	certificate, err := tls.X509KeyPair(cert, key)
	require.NoError(t, err)

	badCACertificate, err := tls.X509KeyPair(badCert, badKey)
	require.NoError(t, err)

	notAllowedCertificate, err := tls.X509KeyPair(notAllowedCert, notAllowedKey)
	require.NoError(t, err)

	tempDir, err := os.MkdirTemp("", "steve_test")
	require.NoError(t, err)
	defer os.Remove(tempDir)

	caFilepath := filepath.Join(tempDir, "request-header-ca.crt")
	certFilepath := filepath.Join(tempDir, "client-auth-proxy.crt")
	keyFilepath := filepath.Join(tempDir, "client-auth-proxy.key")

	os.WriteFile(caFilepath, tinyCA.CertBytes(), 0644)
	os.WriteFile(certFilepath, cert, 0644)
	os.WriteFile(keyFilepath, key, 0644)

	apiServer := &envtest.APIServer{}
	apiServer.Configure().Append("requestheader-allowed-names", "system:auth-proxy")
	apiServer.Configure().Append("requestheader-extra-headers-prefix", "X-Remote-Extra-")
	apiServer.Configure().Append("requestheader-group-headers", "X-Remote-Group")
	apiServer.Configure().Append("requestheader-username-headers", "X-Remote-User")
	apiServer.Configure().Append("requestheader-client-ca-file", caFilepath)
	apiServer.Configure().Append("proxy-client-cert-file", certFilepath)
	apiServer.Configure().Append("proxy-client-key-file", keyFilepath)

	testEnv := envtest.Environment{
		ControlPlane: envtest.ControlPlane{
			APIServer: apiServer,
		},
	}
	restConfig, err := testEnv.Start()
	require.NoError(t, err)

	client, err := kubernetes.NewForConfig(restConfig)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := &controller.SharedControllerFactoryOptions{}
	controllerFactory, err := controller.NewSharedControllerFactoryFromConfigWithOptions(restConfig, scheme, opts)
	require.NoError(t, err)

	ok := wrbacv1.New(controllerFactory)
	accessStore := accesscontrol.NewAccessStore(context.Background(), true, ok)

	err = controllerFactory.Start(ctx, 4)
	require.NoError(t, err)

	store := &authTestStore{
		testStore: &testStore{},
		userCh:    make(chan user.Info, 100),
	}
	_, cleanup, err := setupExtensionAPIServer(t, store, func(opts *ExtensionAPIServerOptions) {
		// XXX: Find a way to get rid of this
		opts.BindPort = 32003
		opts.Client = client
		opts.Authentication = AuthenticationOptions{
			EnableBuiltIn: true,
		}
		opts.Authorization = NewAccessSetAuthorizer(accessStore)
	})
	defer cleanup()

	require.NoError(t, err)

	allPaths := []string{
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
			expectedUser:       &user.DefaultInfo{Name: "my-user", Groups: []string{"my-group", "system:authenticated"}},
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
			name:   "no client certs",
			paths:  allPaths,
			user:   "my-user",
			groups: []string{"my-group"},

			expectedStatusCode: http.StatusUnauthorized,
		},
		{
			name:   "client certs from bad CA",
			certs:  []tls.Certificate{badCACertificate},
			paths:  allPaths,
			user:   "my-user",
			groups: []string{"my-group"},

			expectedStatusCode: http.StatusUnauthorized,
		},
		{
			name:   "client certs with CN not allowed",
			certs:  []tls.Certificate{notAllowedCertificate},
			paths:  allPaths,
			user:   "my-user",
			groups: []string{"my-group"},

			expectedStatusCode: http.StatusUnauthorized,
		},
		{
			name:   "no user",
			paths:  allPaths,
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
					require.NoError(c, err)
					defer resp.Body.Close()

					require.Equal(c, test.expectedStatusCode, resp.StatusCode)
				}, 5*time.Second, 110*time.Millisecond)

				if test.expectedUser != nil {
					authUser, found := store.getUser()
					require.True(t, found)
					require.Equal(t, &user.DefaultInfo{
						Name:   "my-user",
						Groups: []string{"my-group", "system:authenticated"},
						Extra:  map[string][]string{},
					}, authUser)
				}
			}
		})
	}
}
