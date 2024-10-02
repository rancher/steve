package ext

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/authentication/authenticatorfactory"
	"k8s.io/apiserver/pkg/authentication/request/headerrequest"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/kubernetes"
	openapicommon "k8s.io/kube-openapi/pkg/common"
)

const (
	authenticationConfigMapNamespace = metav1.NamespaceSystem
	authenticationConfigMapName      = "extension-apiserver-authentication"
)

// Extracted from https://github.com/kubernetes/apiserver/blob/v0.30.1/pkg/server/options/authentication.go#L301
func ApplyTo(
	client kubernetes.Interface,
	delegatingOptions *options.DelegatingAuthenticationOptions,
	authenticationInfo *server.AuthenticationInfo,
	servingInfo *server.SecureServingInfo,
	openAPIConfig *openapicommon.Config,
) error {
	cfg := authenticatorfactory.DelegatingAuthenticatorConfig{
		Anonymous: &apiserver.AnonymousAuthConfig{
			Enabled: false,
		},
		CacheTTL:                 delegatingOptions.CacheTTL,
		WebhookRetryBackoff:      delegatingOptions.WebhookRetryBackoff,
		TokenAccessReviewTimeout: delegatingOptions.TokenRequestTimeout,
	}
	requestHeaderConfig, err := createRequestHeaderConfig(client)
	if err != nil {
		return fmt.Errorf("requestheaderconfig: %w", err)
	}

	cfg.RequestHeaderConfig = requestHeaderConfig
	authenticationInfo.RequestHeaderConfig = requestHeaderConfig
	if err = authenticationInfo.ApplyClientCert(cfg.RequestHeaderConfig.CAContentProvider, servingInfo); err != nil {
		return fmt.Errorf("unable to load request-header-client-ca-file: %v", err)
	}

	authenticator, securityDefinitions, err := cfg.New()
	if err != nil {
		return err
	}
	authenticationInfo.Authenticator = authenticator
	if openAPIConfig != nil {
		openAPIConfig.SecurityDefinitions = securityDefinitions
	}

	return nil
}

// Copied from https://github.com/kubernetes/apiserver/blob/v0.30.1/pkg/server/options/authentication.go#L407
func createRequestHeaderConfig(client kubernetes.Interface) (*authenticatorfactory.RequestHeaderConfig, error) {
	dynamicRequestHeaderProvider, err := newDynamicRequestHeaderController(client)
	if err != nil {
		return nil, fmt.Errorf("unable to create request header authentication config: %v", err)
	}

	ctx := context.TODO()
	if err := dynamicRequestHeaderProvider.RunOnce(ctx); err != nil {
		return nil, err
	}

	return &authenticatorfactory.RequestHeaderConfig{
		CAContentProvider:   dynamicRequestHeaderProvider,
		UsernameHeaders:     headerrequest.StringSliceProvider(headerrequest.StringSliceProviderFunc(dynamicRequestHeaderProvider.UsernameHeaders)),
		GroupHeaders:        headerrequest.StringSliceProvider(headerrequest.StringSliceProviderFunc(dynamicRequestHeaderProvider.GroupHeaders)),
		ExtraHeaderPrefixes: headerrequest.StringSliceProvider(headerrequest.StringSliceProviderFunc(dynamicRequestHeaderProvider.ExtraHeaderPrefixes)),
		AllowedClientNames:  headerrequest.StringSliceProvider(headerrequest.StringSliceProviderFunc(dynamicRequestHeaderProvider.AllowedClientNames)),
	}, nil
}

// Copied from https://github.com/kubernetes/apiserver/blob/v0.30.1/pkg/server/options/authentication_dynamic_request_header.go#L42
func newDynamicRequestHeaderController(client kubernetes.Interface) (*options.DynamicRequestHeaderController, error) {
	requestHeaderCAController, err := dynamiccertificates.NewDynamicCAFromConfigMapController(
		"client-ca",
		authenticationConfigMapNamespace,
		authenticationConfigMapName,
		"requestheader-client-ca-file",
		client)
	if err != nil {
		return nil, fmt.Errorf("unable to create DynamicCAFromConfigMap controller: %v", err)
	}

	requestHeaderAuthRequestController := headerrequest.NewRequestHeaderAuthRequestController(
		authenticationConfigMapName,
		authenticationConfigMapNamespace,
		client,
		"requestheader-username-headers",
		"requestheader-group-headers",
		"requestheader-extra-headers-prefix",
		"requestheader-allowed-names",
	)
	return &options.DynamicRequestHeaderController{
		ConfigMapCAController:              requestHeaderCAController,
		RequestHeaderAuthRequestController: requestHeaderAuthRequestController,
	}, nil
}
