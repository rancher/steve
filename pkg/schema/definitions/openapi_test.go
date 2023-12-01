package definitions

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
  io.management.cattle.NotAKind:
    type: "string"
    description: "Some string which isn't a kind"
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
`
