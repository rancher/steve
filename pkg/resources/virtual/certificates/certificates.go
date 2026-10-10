package certificates

import (
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TransformCertificate does the following:
func TransformCertificate(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	stateName, stateError, err := getCertificateState(obj)
	if err != nil {
		return obj, err
	}
	unstructured.SetNestedField(obj.Object, stateName, "metadata", "state", "name")
	unstructured.SetNestedField(obj.Object, stateError, "metadata", "state", "error")
	return obj, err
}

// If metadata.state.error is false,
// but the most recent condition is Ready and the status is not True,
// then the certificate is in an error state.
func TransformCertManagerStatus(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	currentErrorStatus, found, err := unstructured.NestedBool(obj.Object, "metadata", "state", "error")
	if err != nil || !found || currentErrorStatus {
		// If the state is reported as error, leave the object as is
		// And if the state block isn't found or it's already reported as error, leave the object as is
		return obj, err
	}
	currentStateName, _, _ := unstructured.NestedString(obj.Object, "metadata", "state", "name")
	pMostRecentCondition, currentErrorStatus, err := getMostRecentCondition(obj)
	if err != nil || currentErrorStatus || pMostRecentCondition == nil {
		return obj, err
	}
	_, adjustedStateName, adjustedErrorStatus := checkReadyStatus(*pMostRecentCondition)
	if adjustedErrorStatus != currentErrorStatus {
		unstructured.SetNestedField(obj.Object, adjustedErrorStatus, "metadata", "state", "error")
	}
	if adjustedStateName != currentStateName {
		unstructured.SetNestedField(obj.Object, adjustedStateName, "metadata", "state", "name")
	}
	return obj, nil
}

func getMostRecentCondition(obj *unstructured.Unstructured) (*map[string]interface{}, bool, error) {
	conditions, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil {
		return nil, true, err
	}
	if !found {
		return nil, false, fmt.Errorf("failed to find status.conditions block in certificate %q", obj.GetName())
	}
	if len(conditions) == 0 {
		return nil, false, fmt.Errorf("status.conditions in certificate %s are empty", obj.GetName())
	}
	condBlock, ok := conditions[len(conditions)-1].(map[string]interface{})
	if !ok {
		return nil, false, fmt.Errorf("failed to parse status.conditions as a map")
	}
	return &condBlock, false, nil
}

func getCertificateState(obj *unstructured.Unstructured) (string, bool, error) {
	pMostRecentCondition, isError, err := getMostRecentCondition(obj)
	if err != nil || isError || pMostRecentCondition == nil {
		return "error", true, err
	}
	mostRecentCondition := *pMostRecentCondition
	readyVal, ok := mostRecentCondition["ready"]
	if ok {
		ready, ok := readyVal.(bool)
		if ok && !ready {
			return "error", false, nil
		}
	}
	failedIssuanceAttemptsVal, ok := mostRecentCondition["failedIssuanceAttempts"]
	if ok {
		failedIssuanceAttempts, ok := failedIssuanceAttemptsVal.(int64)
		if ok && failedIssuanceAttempts > 0 {
			return "error", false, nil
		}
	}
	messageVal, ok := mostRecentCondition["message"]
	if ok {
		message, ok := messageVal.(string)
		if ok && strings.Contains(strings.ToLower(message), "issuing certificate") {
			return "in-progress", false, nil
		}
	}
	notAfterTimestamp, found, err := unstructured.NestedString(obj.Object, "status", "notAfter")
	if found && err == nil {
		notAfterTime, err := time.Parse(time.RFC3339, notAfterTimestamp)
		if err == nil && time.Now().After(notAfterTime) {
			return "expired", true, nil
		}
	}
	renewalTimestamp, found, err := unstructured.NestedString(obj.Object, "status", "renewalTime")
	if found && err == nil {
		renewalTime, err := time.Parse(time.RFC3339, renewalTimestamp)
		if err == nil && time.Now().After(renewalTime) {
			return "expiring", false, nil
		}
	}
	isReady, stateName, isError := checkReadyStatus(mostRecentCondition)
	if isReady {
		return stateName, isError, nil
	}
	return "pending", false, nil
}

// If the most recent condition is Ready:
//
//	If status is True, then everything is in order.
//	If status is False, then the certificate is in error.
//	If status is Unknown, then the certificate is pending.
//
// If the most recent condition is not Ready, then the certificate is in progress.
func checkReadyStatus(mostRecentCondition map[string]any) (isReady bool, typeName string, isError bool) {
	typeNameValue, ok := mostRecentCondition["type"]
	if !ok {
		return false, "", false
	}
	typeName, ok = typeNameValue.(string)
	if !ok || typeName != "Ready" {
		return false, "in-progress", false
	}
	if statusValue, ok := mostRecentCondition["status"]; ok {
		if status, ok := statusValue.(string); ok {
			switch status {
			case "True":
				return true, "active", false
			case "False":
				return true, "error", true
			case "Unknown":
				return true, "pending", false
			}
		}
	}
	return false, "error", true
}
