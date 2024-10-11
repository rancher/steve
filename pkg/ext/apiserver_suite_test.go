package ext

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

type ExtensionAPIServerSuite struct {
	suite.Suite

	ctx    context.Context
	cancel context.CancelFunc

	testEnv    envtest.Environment
	client     *kubernetes.Clientset
	restConfig *rest.Config
}

func (s *ExtensionAPIServerSuite) SetupSuite() {
	var err error
	apiServer := &envtest.APIServer{}

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
	s.cancel()
	err := s.testEnv.Stop()
	s.Require().NoError(err)
}

func TestExtensionAPIServerSuite(t *testing.T) {
	suite.Run(t, new(ExtensionAPIServerSuite))
}
