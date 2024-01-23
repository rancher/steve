package definitions

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/wrangler/v2/pkg/generic/fake"
	"github.com/stretchr/testify/require"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiregv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

func TestRegister(t *testing.T) {
	schemas := types.EmptyAPISchemas()
	client := fakeDiscovery{}
	ctrl := gomock.NewController(t)
	crdController := fake.NewMockNonNamespacedControllerInterface[*apiextv1.CustomResourceDefinition, *apiextv1.CustomResourceDefinitionList](ctrl)
	apisvcController := fake.NewMockNonNamespacedControllerInterface[*apiregv1.APIService, *apiregv1.APIServiceList](ctrl)
	ctx, cancel := context.WithCancel(context.Background())
	crdController.EXPECT().OnChange(ctx, handlerKey, gomock.Any())
	apisvcController.EXPECT().OnChange(ctx, handlerKey, gomock.Any())
	Register(ctx, schemas, &client, crdController, apisvcController)
	registeredSchema := schemas.LookupSchema("schemaDefinition")
	require.NotNil(t, registeredSchema)
	require.Len(t, registeredSchema.ResourceMethods, 1)
	require.Equal(t, registeredSchema.ResourceMethods[0], "GET")
	require.NotNil(t, registeredSchema.ByIDHandler)
	// Register will spawn a background thread, so we want to stop that to not impact other tests
	cancel()
}

func Test_getDurationEnvVarOrDefault(t *testing.T) {
	os.Setenv("VALID", "1")
	os.Setenv("INVALID", "NOTANUMBER")
	tests := []struct {
		name         string
		envVar       string
		defaultValue int
		unit         time.Duration
		wantDuration time.Duration
	}{
		{
			name:         "not found, use default",
			envVar:       "NOT_FOUND",
			defaultValue: 12,
			unit:         time.Second,
			wantDuration: time.Second * 12,
		},
		{
			name:         "found but not an int",
			envVar:       "INVALID",
			defaultValue: 24,
			unit:         time.Minute,
			wantDuration: time.Minute * 24,
		},
		{
			name:         "found and valid int",
			envVar:       "VALID",
			defaultValue: 30,
			unit:         time.Hour,
			wantDuration: time.Hour * 1,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := getDurationEnvVarOrDefault(test.envVar, test.defaultValue, test.unit)
			require.Equal(t, test.wantDuration, got)
		})
	}
}
