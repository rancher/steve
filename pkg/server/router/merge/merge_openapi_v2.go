package merge

import (
	"fmt"
	"slices"

	openapiv2 "github.com/google/gnostic-models/openapiv2"
	"sigs.k8s.io/yaml"
)

func mergeSlicesOnValue[T comparable, C comparable](lhs, rhs []T, getter func(T) C) []T {
	if len(lhs) == 0 {
		return rhs
	}

	if len(rhs) == 0 {
		return lhs
	}

	for _, t := range lhs {
		found := slices.ContainsFunc(lhs, func(v T) bool {
			return getter(v) == getter(t)
		})

		if !found {
			lhs = append(lhs, t)
		}
	}

	return lhs
}

func mergeSlices[T comparable](lhs, rhs []T) []T {
	return mergeSlicesOnValue(lhs, rhs, func(t T) T {
		return t
	})
}

func mergeOpenApiV2Document(lhs, rhs *openapiv2.Document) (*openapiv2.Document, error) {
	if lhs.Swagger != rhs.Swagger {
		return nil, fmt.Errorf("found mismatched swagger versions: '%s' != '%s'", lhs.Swagger, rhs.Swagger)
	}

	lhs.Schemes = mergeSlices(lhs.Schemes, rhs.Schemes)
	lhs.Consumes = mergeSlices(lhs.Consumes, rhs.Consumes)
	lhs.Produces = mergeSlices(lhs.Produces, rhs.Produces)

	if lhs.Paths != nil && rhs.Paths != nil {
		lhs.Paths.Path = mergeSlicesOnValue(lhs.Paths.Path, rhs.Paths.Path, func(v *openapiv2.NamedPathItem) string {
			return v.Name
		})
	} else if rhs != nil {
		lhs.Paths = rhs.Paths
	}

	lhs.VendorExtension = mergeSlicesOnValue(lhs.Paths.VendorExtension, rhs.Paths.VendorExtension, func(v *openapiv2.NamedAny) string {
		return v.Name
	})

	if lhs.Definitions != nil && rhs.Definitions != nil {
		lhs.Definitions.AdditionalProperties = mergeSlicesOnValue(lhs.Definitions.AdditionalProperties, rhs.Definitions.AdditionalProperties, func(v *openapiv2.NamedSchema) string {
			return v.Name
		})
	} else if rhs != nil {
		lhs.Definitions = rhs.Definitions
	}

	if lhs.Parameters != nil && rhs.Parameters != nil {
		lhs.Parameters.AdditionalProperties = mergeSlicesOnValue(lhs.Parameters.AdditionalProperties, rhs.Parameters.AdditionalProperties, func(v *openapiv2.NamedParameter) string {
			return v.Name
		})
	} else if rhs != nil {
		lhs.Parameters = rhs.Parameters
	}

	if lhs.Responses != nil && rhs.Responses != nil {
		lhs.Responses.AdditionalProperties = mergeSlicesOnValue(lhs.Responses.AdditionalProperties, rhs.Responses.AdditionalProperties, func(v *openapiv2.NamedResponse) string {
			return v.Name
		})
	} else if rhs != nil {
		lhs.Responses = rhs.Responses
	}

	lhs.Security = mergeSlices(lhs.Security, rhs.Security)

	if lhs.SecurityDefinitions != nil && rhs.SecurityDefinitions != nil {
		lhs.SecurityDefinitions.AdditionalProperties = mergeSlicesOnValue(lhs.SecurityDefinitions.AdditionalProperties, rhs.SecurityDefinitions.AdditionalProperties, func(v *openapiv2.NamedSecurityDefinitionsItem) string {
			return v.Name
		})
	} else if rhs != nil {
		lhs.SecurityDefinitions = rhs.SecurityDefinitions
	}

	lhs.Tags = mergeSlicesOnValue(lhs.Tags, rhs.Tags, func(v *openapiv2.Tag) string {
		return v.Name
	})

	if lhs.ExternalDocs != nil && rhs.ExternalDocs != nil {
		return nil, fmt.Errorf("unable to merge two non-nil ExternalDocs fields")
	} else if rhs.ExternalDocs != nil {
		lhs.ExternalDocs = rhs.ExternalDocs
	}

	if lhs.VendorExtension != nil && rhs.VendorExtension != nil {
		lhs.VendorExtension = mergeSlicesOnValue(lhs.VendorExtension, rhs.VendorExtension, func(v *openapiv2.NamedAny) string {
			return v.Name
		})
	} else if rhs.VendorExtension != nil {
		lhs.VendorExtension = rhs.VendorExtension
	}

	return lhs, nil
}

func OpenAPIV2Merger(primaryData []byte, secondaryData []byte) ([]byte, error) {
	primaryDocument, err := openapiv2.ParseDocument(primaryData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse document: %w", err)
	}

	secondaryDocument, err := openapiv2.ParseDocument(secondaryData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse document: %w", err)
	}

	merged, err := mergeOpenApiV2Document(primaryDocument, secondaryDocument)
	if err != nil {
		return nil, fmt.Errorf("failed to merge openapi documents: %w", err)
	}

	data, err := merged.YAMLValue("")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal merged response: %w", err)
	}

	data, err = yaml.YAMLToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal json response: %w", err)
	}

	return data, nil
}
