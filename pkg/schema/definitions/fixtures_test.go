package definitions

import (
	"bytes"
	"fmt"

	"github.com/rancher/wrangler/v3/pkg/yaml"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

var (
	rawCRDs = `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: userattributes.management.cattle.io
spec:
  conversion:
    strategy: None
  group: management.cattle.io
  names:
    kind: UserAttribute
    listKind: UserAttributeList
    plural: userattributes
    singular: userattribute
  scope: Cluster
  versions:
  - name: v2
    schema:
      openAPIV3Schema:
        type: object
        x-kubernetes-preserve-unknown-fields: true
    served: true
    storage: true
---
kind: CustomResourceDefinition
metadata:
  name: nullable.management.cattle.io
spec:
  conversion:
    strategy: None
  group: management.cattle.io
  names:
    kind: Nullable
    listKind: NullableList
    plural: nullables
    singular: nullable
  scope: Cluster
  versions:
  - name: v2
    schema:
      openAPIV3Schema:
        type: object
        properties:
          spec:
            type: object
            properties:
              rkeConfig:
                type: object
                nullable: true
                properties:
                  additionalManifest:
                    type: string
                    nullable: true
    served: true
    storage: true
`
)

func getCRDs() ([]*apiextv1.CustomResourceDefinition, error) {
	crds, err := yaml.UnmarshalWithJSONDecoder[*apiextv1.CustomResourceDefinition](bytes.NewBuffer([]byte(rawCRDs)))
	if err != nil {
		return nil, fmt.Errorf("unmarshal CRD: %w", err)
	}
	return crds, err
}

const openapi_raw = `
swagger: "2.0"
info:
  title: "Test openapi spec"
  version: "v1.0.0"
paths:
  /apis/management.cattle.io/v3/globalroles:
    get:
      description: "get a global role"
      responses:
        200:
          description: "OK"
definitions:
  io.cattle.management.v1.GlobalRole:
    description: "A Global Role V1 provides Global Permissions in Rancher"
    type: "object"
    properties:
      apiVersion:
        description: "The APIVersion of this resource"
        type: "string"
      kind:
        description: "The kind"
        type: "string"
      metadata:
        description: "The metadata"
        $ref: "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"
      spec:
        description: "The spec for the project"
        type: "object"
        required:
        - "clusterName"
        - "displayName"
        properties:
          clusterName:
            description: "The name of the cluster"
            type: "string"
          displayName:
            description: "The UI readable name"
            type: "string"
          notRequired:
            description: "Some field that isn't required"
            type: "boolean"
    x-kubernetes-group-version-kind:
    - group: "management.cattle.io"
      version: "v1"
      kind: "GlobalRole"
  io.cattle.management.v2.GlobalRole:
    description: "A Global Role V2 provides Global Permissions in Rancher"
    type: "object"
    properties:
      apiVersion:
        description: "The APIVersion of this resource"
        type: "string"
      kind:
        description: "The kind"
        type: "string"
      metadata:
        description: "The metadata"
        $ref: "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"
      spec:
        description: "The spec for the project"
        type: "object"
        required:
        - "clusterName"
        - "displayName"
        properties:
          clusterName:
            description: "The name of the cluster"
            type: "string"
          displayName:
            description: "The UI readable name"
            type: "string"
          notRequired:
            description: "Some field that isn't required"
            type: "boolean"
          newField:
            description: "A new field not present in v1"
            type: "string"
    x-kubernetes-group-version-kind:
    - group: "management.cattle.io"
      version: "v2"
      kind: "GlobalRole"
  io.cattle.management.v2.NewResource:
    description: "A resource that's in the v2 group, but not v1"
    type: "object"
    properties:
      apiVersion:
        description: "The APIVersion of this resource"
        type: "string"
      kind:
        description: "The kind"
        type: "string"
      metadata:
        description: "The metadata"
        $ref: "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"
      spec:
        description: "The spec for the new resource"
        type: "object"
        required:
        - "someRequired"
        properties:
          someRequired:
            description: "A required field"
            type: "string"
          notRequired:
            description: "Some field that isn't required"
            type: "boolean"
    x-kubernetes-group-version-kind:
    - group: "management.cattle.io"
      version: "v2"
      kind: "NewResource"
  io.cattle.noversion.v2.Resource:
    description: "A No Version V2 resource is for a group with no preferred version"
    type: "object"
    properties:
      apiVersion:
        description: "The APIVersion of this resource"
        type: "string"
      kind:
        description: "The kind"
        type: "string"
      metadata:
        description: "The metadata"
        $ref: "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"
      spec:
        description: "The spec for the resource"
        type: "object"
        required:
        - "name"
        properties:
          name:
            description: "The name of the resource"
            type: "string"
          notRequired:
            description: "Some field that isn't required"
            type: "boolean"
          newField:
            description: "A new field not present in v1"
            type: "string"
    x-kubernetes-group-version-kind:
    - group: "noversion.cattle.io"
      version: "v2"
      kind: "Resource"
  io.cattle.noversion.v1.Resource:
    description: "A No Version V1 resource is for a group with no preferred version"
    type: "object"
    properties:
      apiVersion:
        description: "The APIVersion of this resource"
        type: "string"
      kind:
        description: "The kind"
        type: "string"
      metadata:
        description: "The metadata"
        $ref: "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"
      spec:
        description: "The spec for the resource"
        type: "object"
        required:
        - "name"
        properties:
          name:
            description: "The name of the resource"
            type: "string"
          notRequired:
            description: "Some field that isn't required"
            type: "boolean"
    x-kubernetes-group-version-kind:
    - group: "noversion.cattle.io"
      version: "v1"
      kind: "Resource"
  io.cattle.management.v1.DeprecatedResource:
    description: "A resource that is not present in v2"
    type: "object"
    properties:
      apiVersion:
        description: "The APIVersion of this resource"
        type: "string"
      kind:
        description: "The kind"
        type: "string"
      metadata:
        description: "The metadata"
        $ref: "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"
    x-kubernetes-group-version-kind:
    - group: "management.cattle.io"
      version: "v1"
      kind: "DeprecatedResource"
  io.cattle.missinggroup.v2.Resource:
    description: "A Missing Group V2 resource is for a group not listed by server groups"
    type: "object"
    properties:
      apiVersion:
        description: "The APIVersion of this resource"
        type: "string"
      kind:
        description: "The kind"
        type: "string"
      metadata:
        description: "The metadata"
        $ref: "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"
      spec:
        description: "The spec for the resource"
        type: "object"
        required:
        - "name"
        properties:
          name:
            description: "The name of the resource"
            type: "string"
          notRequired:
            description: "Some field that isn't required"
            type: "boolean"
          newField:
            description: "A new field not present in v1"
            type: "string"
    x-kubernetes-group-version-kind:
    - group: "missinggroup.cattle.io"
      version: "v2"
      kind: "Resource"
  io.cattle.missinggroup.v1.Resource:
    description: "A Missing Group V1 resource is for a group not listed by server groups"
    type: "object"
    properties:
      apiVersion:
        description: "The APIVersion of this resource"
        type: "string"
      kind:
        description: "The kind"
        type: "string"
      metadata:
        description: "The metadata"
        $ref: "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"
      spec:
        description: "The spec for the resource"
        type: "object"
        required:
        - "name"
        properties:
          name:
            description: "The name of the resource"
            type: "string"
          notRequired:
            description: "Some field that isn't required"
            type: "boolean"
    x-kubernetes-group-version-kind:
    - group: "missinggroup.cattle.io"
      version: "v1"
      kind: "Resource"
  io.cattle.management.v2.Nullable:
    type: "object"
    description: ""
    properties:
      apiVersion:
        description: "The APIVersion of this resource"
        type: "string"
      kind:
        description: "The kind"
        type: "string"
      metadata:
        description: "The metadata"
        $ref: "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"
      spec:
        description: ""
        type: "object"
        properties:
          rkeConfig:
    x-kubernetes-group-version-kind:
    - group: "management.cattle.io"
      version: "v2"
      kind: "Nullable"
  io.cattle.management.NotAKind:
    type: "string"
    description: "Some string which isn't a kind"
  io.cattle.management.v2.UserAttribute:
    type: "object"
    x-kubernetes-group-version-kind:
    - group: "management.cattle.io"
      version: "v2"
      kind: "UserAttribute"
  io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta:
    description: "Object Metadata"
    properties:
      annotations:
        description: "annotations of the resource"
        type: "object"
        additionalProperties:
          type: "string"
      name:
        description: "name of the resource"
        type: "string"
  io.k8s.api.core.v1.ConfigMap:
    type: "object"
    description: "ConfigMap holds configuration data for pods to consume."
    properties:
      apiVersion:
        description: "APIVersion defines the versioned schema of this representation of an object. Servers should convert recognized schemas to the latest internal value, and may reject unrecognized values. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources"
        type: "string"
      kind:
        description: "Kind is a string value representing the REST resource this object represents. Servers may infer this from the endpoint the client submits requests to. Cannot be updated. In CamelCase. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds"
        type: "string"
      metadata:
        description: "Standard object's metadata. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata"
        $ref: "#/definitions/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta"
      binaryData:
        description: "BinaryData contains the binary data. Each key must consist of alphanumeric characters, '-', '_' or '.'. BinaryData can contain byte sequences that are not in the UTF-8 range. The keys stored in BinaryData must not overlap with the ones in the Data field, this is enforced during validation process. Using this field will require 1.10+ apiserver and kubelet."
        type: "object"
        additionalProperties:
          type: "string"
          format: "byte"
      data:
        description: "Data contains the configuration data. Each key must consist of alphanumeric characters, '-', '_' or '.'. Values with non-UTF-8 byte sequences must use the BinaryData field. The keys stored in Data must not overlap with the keys in the BinaryData field, this is enforced during validation process."
        type: "object"
        additionalProperties:
          type: "string"
      immutable:
        description: "Immutable, if set to true, ensures that data stored in the ConfigMap cannot be updated (only object metadata can be modified). If not set to true, the field can be modified at any time. Defaulted to nil."
        type: "boolean"
    x-kubernetes-group-version-kind:
    - group: ""
      kind: "ConfigMap"
      version: "v1"
`
