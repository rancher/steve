package acmechallenges

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TransformCertificate does the following:
// convert names
// if status.state is pending and status.processing is true, set the state to "in-progress"

func TransformChallenge(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	var statusStateName string
	var statusProcessing bool

	statusBlock, found, err := unstructured.NestedMap(obj.Object, "status")
	if found && err == nil {
		statusStateName, found, err = unstructured.NestedString(statusBlock, "state")
		if found && err == nil && statusStateName == "pending" {
			statusProcessing, found, err = unstructured.NestedBool(statusBlock, "processing")
			if found && err == nil && statusProcessing {
				unstructured.SetNestedField(obj.Object, "in-progress", "metadata", "state", "name")
				return obj, nil
			}
		}
	}
	currentStateName, _, _ := unstructured.NestedString(obj.Object, "metadata", "state", "name")
	currentStateError, _, _ := unstructured.NestedBool(obj.Object, "metadata", "state", "error")
	// Ignore errors here
	// Trying to set metadata.state.* will create the block
	fixedName := statusStateName
	fixedError := currentStateError
	switch statusStateName {
	case "valid":
		fixedName = "active"
	case "ready":
		fixedName = "in-progress"
	case "processing":
		fixedName = "in-progress"
	case "invalid":
		fixedName = "error"
		fixedError = true
	case "errored":
		fixedName = "error"
		fixedError = true
	case "denied":
		fixedError = true
	case "expired":
	case "expiring":
	case "pending":
	default:
		fixedName = "unknown"
	}
	if fixedName != currentStateName {
		unstructured.SetNestedField(obj.Object, fixedName, "metadata", "state", "name")
	}
	if fixedError != currentStateError {
		unstructured.SetNestedField(obj.Object, fixedError, "metadata", "state", "error")
	}
	return obj, nil
}
