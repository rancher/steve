package ext

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	authenticatorunion "k8s.io/apiserver/pkg/authentication/request/union"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/openapi"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	utilversion "k8s.io/apiserver/pkg/util/version"
	"k8s.io/client-go/kubernetes"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

var (
	schemeBuilder = runtime.NewSchemeBuilder(addKnownTypes, metainternalversion.AddToScheme)
	AddToScheme   = schemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	metav1.AddToGroupVersion(scheme, schema.GroupVersion{Version: "v1"})
	return nil
}

type ExtensionAPIServerOptions struct {
	// GetOpenAPIDefinitions is collection of all definitions. Required.
	GetOpenAPIDefinitions             openapicommon.GetOpenAPIDefinitions
	OpenAPIDefinitionNameReplacements map[string]string

	Authentication AuthenticationOptions

	Authorization authorizer.Authorizer

	Client kubernetes.Interface

	BindPort int
}

type AuthenticationOptions struct {
	// When turned off, reject requests, unless custom authentication passes
	EnableBuiltIn       bool
	CustomAuthenticator authenticator.RequestFunc
}

// ExtensionAPIServer wraps a [genericapiserver.GenericAPIServer] to implement
// a Kubernetes extension API server.
//
// Use [InstallStore] to add a new resource store onto an existing ExtensionAPIServer.
// Each resources will then be reachable via /apis/<group>/<version>/<resource> as
// defined by the Kubernetes API.
type ExtensionAPIServer struct {
	codecs serializer.CodecFactory
	scheme *runtime.Scheme

	genericAPIServer *genericapiserver.GenericAPIServer
	apiGroups        map[string]genericapiserver.APIGroupInfo

	authorizer authorizer.Authorizer

	handlerMu sync.RWMutex
	handler   http.Handler
}

type emptyAddresses struct{}

func (e emptyAddresses) ServerAddressByClientCIDRs(clientIP net.IP) []metav1.ServerAddressByClientCIDR {
	return nil
}

func NewExtensionAPIServer(scheme *runtime.Scheme, codecs serializer.CodecFactory, opts ExtensionAPIServerOptions) (*ExtensionAPIServer, error) {
	recommendedOpts := genericoptions.NewRecommendedOptions("", codecs.LegacyCodec())

	if opts.Authorization == nil {
		return nil, fmt.Errorf("no authorization provided")
	}

	resolver := &request.RequestInfoFactory{APIPrefixes: sets.NewString("apis", "api"), GrouplessAPIPrefixes: sets.NewString("api")}
	config := genericapiserver.NewRecommendedConfig(codecs)
	config.RequestInfoResolver = resolver
	config.Authorization = genericapiserver.AuthorizationInfo{
		Authorizer: opts.Authorization,
	}

	// XXX: kubectl doesn't show this, and it's listed as optional so it
	//      should be safe to remove. I haven't found a lot of resource on
	//      what this does.
	config.DiscoveryAddresses = emptyAddresses{}
	config.EffectiveVersion = utilversion.NewEffectiveVersion("")

	config.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(opts.GetOpenAPIDefinitions, openapi.NewDefinitionNamer(scheme))
	config.OpenAPIConfig.Info.Title = "Ext"
	config.OpenAPIConfig.Info.Version = "0.1"
	config.OpenAPIConfig.GetDefinitionName = getDefinitionName(scheme, opts.OpenAPIDefinitionNameReplacements)
	// Must set to nil otherwise getDefinitionName won't be used for refs
	// which will break kubectl explain
	config.OpenAPIConfig.Definitions = nil

	config.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(opts.GetOpenAPIDefinitions, openapi.NewDefinitionNamer(scheme))
	config.OpenAPIV3Config.Info.Title = "Ext"
	config.OpenAPIV3Config.Info.Version = "0.1"
	config.OpenAPIV3Config.GetDefinitionName = getDefinitionName(scheme, opts.OpenAPIDefinitionNameReplacements)
	// Must set to nil otherwise getDefinitionName won't be used for refs
	// which will break kubectl explain
	config.OpenAPIV3Config.Definitions = nil

	recommendedOpts.SecureServing.BindPort = opts.BindPort
	if err := recommendedOpts.SecureServing.ApplyTo(&config.SecureServing, &config.LoopbackClientConfig); err != nil {
		return nil, fmt.Errorf("applyto secureserving: %w", err)
	}

	if !opts.Authentication.EnableBuiltIn && opts.Authentication.CustomAuthenticator == nil {
		return nil, fmt.Errorf("at least one authenticator must be configured")
	}

	if opts.Authentication.EnableBuiltIn {
		if opts.Client == nil {
			return nil, fmt.Errorf("client required for builtin auth")
		}

		if err := ApplyTo(
			opts.Client,
			recommendedOpts.Authentication,
			&config.Authentication,
			config.SecureServing,
			config.OpenAPIConfig,
		); err != nil {
			return nil, fmt.Errorf("applyto authentication: %w", err)
		}
	}
	if opts.Authentication.CustomAuthenticator != nil {
		if opts.Authentication.EnableBuiltIn {
			config.Authentication.Authenticator = authenticatorunion.New(
				opts.Authentication.CustomAuthenticator,
				config.Authentication.Authenticator,
			)
		} else {
			config.Authentication = genericapiserver.AuthenticationInfo{
				Authenticator: opts.Authentication.CustomAuthenticator,
			}
		}
	}

	completedConfig := config.Complete()
	genericServer, err := completedConfig.New("imperative-api", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return nil, fmt.Errorf("new: %w", err)
	}

	extensionAPIServer := &ExtensionAPIServer{
		codecs:           codecs,
		scheme:           scheme,
		genericAPIServer: genericServer,
		apiGroups:        make(map[string]genericapiserver.APIGroupInfo),
		authorizer:       opts.Authorization,
	}

	return extensionAPIServer, nil
}

// Run prepares and runs the separate HTTPS server. It also configures the handler
// so that ServeHTTP can be used.
func (s *ExtensionAPIServer) Run(ctx context.Context, readyCh chan struct{}) error {
	for _, apiGroup := range s.apiGroups {
		err := s.genericAPIServer.InstallAPIGroup(&apiGroup)
		if err != nil {
			return fmt.Errorf("installgroup: %w", err)
		}
	}
	prepared := s.genericAPIServer.PrepareRun()
	s.handlerMu.Lock()
	s.handler = prepared.Handler
	s.handlerMu.Unlock()

	readyCh <- struct{}{}

	if err := prepared.RunWithContext(ctx); err != nil {
		return err
	}

	return nil
}

func (s *ExtensionAPIServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.handlerMu.RLock()
	defer s.handlerMu.RUnlock()
	s.handler.ServeHTTP(w, req)
}

// InstallStore installs a store on the given ExtensionAPIServer object.
//
// t and TList must be non-nil.
//
// Here's an example store for a Token and TokenList resource in the ext.cattle.io/v1 apiVersion:
//
//	gvk := schema.GroupVersionKind{
//		Group: "ext.cattle.io",
//		Version: "v1",
//		Kind: "Token",
//	}
//	InstallStore(s, &Token{}, &TokenList{}, "tokens", "token", gvk, store)
//
// Note: Not using a method on ExtensionAPIServer object due to Go generic limitations.
func InstallStore[T runtime.Object, TList runtime.Object](
	s *ExtensionAPIServer,
	t T,
	tList TList,
	resourceName string,
	singularName string,
	gvk schema.GroupVersionKind,
	store Store[T, TList],
) {
	apiGroup, ok := s.apiGroups[gvk.Group]
	if !ok {
		apiGroup = genericapiserver.NewDefaultAPIGroupInfo(gvk.Group, s.scheme, metav1.ParameterCodec, s.codecs)
	}

	_, ok = apiGroup.VersionedResourcesStorageMap[gvk.Version]
	if !ok {
		apiGroup.VersionedResourcesStorageMap[gvk.Version] = make(map[string]rest.Storage)
	}

	delegate := &delegate[T, TList]{
		scheme: s.scheme,

		t:            t,
		tList:        tList,
		singularName: singularName,
		gvk:          gvk,
		gvr: schema.GroupVersionResource{
			Group:    gvk.Group,
			Version:  gvk.Version,
			Resource: resourceName,
		},
		authorizer: s.authorizer,
		store:      store,
	}

	apiGroup.VersionedResourcesStorageMap[gvk.Version][resourceName] = delegate
	s.apiGroups[gvk.Group] = apiGroup
}

func getDefinitionName(scheme *runtime.Scheme, replacements map[string]string) func(string) (string, spec.Extensions) {
	return func(name string) (string, spec.Extensions) {
		namer := openapi.NewDefinitionNamer(scheme)
		definitionName, defGVK := namer.GetDefinitionName(name)
		for key, val := range replacements {
			if !strings.HasPrefix(definitionName, key) {
				continue
			}

			updatedName := strings.ReplaceAll(definitionName, key, val)
			return updatedName, defGVK
		}
		return definitionName, defGVK
	}
}
