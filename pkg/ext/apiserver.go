package ext

import (
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/rancher/wrangler/v3/pkg/schemes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/endpoints/openapi"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

var (
	// Reference:
	// Scheme scheme for unversioned types - such as APIResourceList, and Status
	scheme = runtime.NewScheme()
	// Codecs for unversioned types - such as APIResourceList, and Status
	Codecs = serializer.NewCodecFactory(scheme)
)

func init() {
	schemes.AddToScheme(scheme)
	metav1.AddToGroupVersion(scheme, schema.GroupVersion{Version: "v1"})
	// // Needed for CreateOptions, UpdateOptions, etc for ParameterCodec
	// internalversion.AddToScheme(scheme)

	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	scheme.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)
}

type ExtensionAPIServerOptions struct {
	// GetOpenAPIDefinitions is collection of all definitions. Required.
	GetOpenAPIDefinitions             openapicommon.GetOpenAPIDefinitions
	OpenAPIDefinitionNameReplacements map[string]string

	CustomAuthenticator authenticator.RequestFunc
}

type ExtensionAPIServer struct {
	codecs serializer.CodecFactory
	scheme *runtime.Scheme

	genericAPIServer *genericapiserver.GenericAPIServer
	apiGroups        map[string]genericapiserver.APIGroupInfo

	handler http.Handler
}

type emptyAddresses struct{}

func (e emptyAddresses) ServerAddressByClientCIDRs(clientIP net.IP) []metav1.ServerAddressByClientCIDR {
	return nil
}

func NewExtensionAPIServer(scheme *runtime.Scheme, codecs serializer.CodecFactory, opts ExtensionAPIServerOptions) (*ExtensionAPIServer, error) {
	recommendedOpts := genericoptions.NewRecommendedOptions("", codecs.LegacyCodec())

	// XXX: Need to call config.Complete() -> This sets the resolver for example

	resolver := &request.RequestInfoFactory{APIPrefixes: sets.NewString("apis", "api"), GrouplessAPIPrefixes: sets.NewString("api")}
	config := genericapiserver.NewRecommendedConfig(codecs)
	config.RequestInfoResolver = resolver
	if opts.CustomAuthenticator != nil {
		config.Authentication = genericapiserver.AuthenticationInfo{
			Authenticator: opts.CustomAuthenticator,
		}
	}
	config.ExternalAddress = ":4444"

	// XXX: kubectl doesn't show this, why is it here, do we need it?
	// XXX: Understand what this is for
	config.DiscoveryAddresses = emptyAddresses{}

	config.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(opts.GetOpenAPIDefinitions, openapi.NewDefinitionNamer(scheme))
	config.OpenAPIConfig.Info.Title = "Ext"
	config.OpenAPIConfig.Info.Version = "0.1"
	config.OpenAPIConfig.GetDefinitionName = getDefinitionName(opts.OpenAPIDefinitionNameReplacements)
	// Must set to nil otherwise getDefinitionName won't be used for refs
	// which will break kubectl explain
	config.OpenAPIConfig.Definitions = nil

	config.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(opts.GetOpenAPIDefinitions, openapi.NewDefinitionNamer(scheme))
	config.OpenAPIV3Config.Info.Title = "Ext"
	config.OpenAPIV3Config.Info.Version = "0.1"
	config.OpenAPIV3Config.GetDefinitionName = getDefinitionName(opts.OpenAPIDefinitionNameReplacements)
	// Must set to nil otherwise getDefinitionName won't be used for refs
	// which will break kubectl explain
	config.OpenAPIV3Config.Definitions = nil

	recommendedOpts.SecureServing.BindPort = 4445
	recommendedOpts.SecureServing.PermitPortSharing = true
	if err := recommendedOpts.SecureServing.ApplyTo(&config.SecureServing, &config.LoopbackClientConfig); err != nil {
		return nil, fmt.Errorf("applyto secureservice: %w", err)
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
	}

	return extensionAPIServer, nil
}

func (s *ExtensionAPIServer) InstallResourceStore(store ResourceStore) {
	apiGroup, ok := s.apiGroups[store.gvk.Group]
	if !ok {
		apiGroup = genericapiserver.NewDefaultAPIGroupInfo(store.gvk.Group, s.scheme, metav1.ParameterCodec, s.codecs)
	}

	_, ok = apiGroup.VersionedResourcesStorageMap[store.gvk.Version]
	if !ok {
		apiGroup.VersionedResourcesStorageMap[store.gvk.Version] = make(map[string]rest.Storage)
	}

	apiGroup.VersionedResourcesStorageMap[store.gvk.Version][store.name] = store.storage
	s.apiGroups[store.gvk.Group] = apiGroup
}

func (s *ExtensionAPIServer) Prepare() error {
	for _, apiGroup := range s.apiGroups {
		err := s.genericAPIServer.InstallAPIGroup(&apiGroup)
		if err != nil {
			return fmt.Errorf("installgroup: %w", err)
		}
	}
	prepared := s.genericAPIServer.PrepareRun()
	s.handler = prepared.Handler
	return nil
}

func (s *ExtensionAPIServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.handler.ServeHTTP(w, req)
}

type ResourceStore struct {
	// Name plural name of the resource (eg: tokens)
	name string
	// SingularName singular name of the resource (eg: token)
	singularName string
	// GVK is the group, version, kind of the resource
	gvk     schema.GroupVersionKind
	storage rest.Storage
}

func MakeResourceStore[
	T Ptr[DerefT],
	DerefT any,
	TList Ptr[DerefTList],
	DerefTList any,
](resourceName string, singularName string, gvk schema.GroupVersionKind, store Store[T, TList]) ResourceStore {
	s := &delegate[T, DerefT, TList, DerefTList]{
		singularName: singularName,
		gvk:          gvk,
		gvr: schema.GroupVersionResource{
			Group:    gvk.Group,
			Version:  gvk.Version,
			Resource: resourceName,
		},
		store: store,
	}

	resourceStore := ResourceStore{
		name:         resourceName,
		singularName: singularName,
		gvk:          gvk,
		storage:      s,
	}
	return resourceStore
}

func getDefinitionName(replacements map[string]string) func(string) (string, spec.Extensions) {
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
