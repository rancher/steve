package ext

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	crand "crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/client-go/rest"
	certutil "k8s.io/client-go/util/cert"

	"github.com/stretchr/testify/suite"
	"k8s.io/client-go/kubernetes"
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

type ExtensionAPIServerSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc

	testEnv    envtest.Environment
	client     *kubernetes.Clientset
	restConfig *rest.Config

	certTempPath string
	ca           *TinyCA
	cert         CertPair
}

func (s *ExtensionAPIServerSuite) SetupSuite() {
	var err error
	s.ca, err = NewTinyCA()
	s.Require().NoError(err)
	s.cert, err = s.ca.NewClientCert("system:auth-proxy")
	s.Require().NoError(err)

	cert, key, err := s.cert.AsBytes()
	s.Require().NoError(err)

	s.certTempPath, err = os.MkdirTemp("", "steve_test")
	s.Require().NoError(err)

	caFilepath := filepath.Join(s.certTempPath, "request-header-ca.crt")
	certFilepath := filepath.Join(s.certTempPath, "client-auth-proxy.crt")
	keyFilepath := filepath.Join(s.certTempPath, "client-auth-proxy.key")

	os.WriteFile(caFilepath, s.ca.CertBytes(), 0644)
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

	s.testEnv = envtest.Environment{
		ControlPlane: envtest.ControlPlane{
			APIServer: apiServer,
		},
	}
	s.restConfig, err = s.testEnv.Start()
	s.Require().NoError(err)

	s.client, err = kubernetes.NewForConfig(s.restConfig)
	s.Require().NoError(err)

	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *ExtensionAPIServerSuite) TearDownSuite() {
	os.RemoveAll(s.certTempPath)
	err := s.testEnv.Stop()
	s.Require().NoError(err)
	s.cancel()
}

func TestExtensionAPIServerSuite(t *testing.T) {
	suite.Run(t, new(ExtensionAPIServerSuite))
}
