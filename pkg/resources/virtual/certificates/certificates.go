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

func getCertificateState(obj *unstructured.Unstructured) (string, bool, error) {
	conditions, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil {
		return "", false, err
	}
	if !found {
		return "", true, fmt.Errorf("failed to find status.conditions block in certificate %q", obj.GetName())
	}
	mostRecentCondition := conditions[len(conditions)-1].(map[string]interface{})
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
	typeNameValue, ok := mostRecentCondition["type"]
	if ok {
		typeName, ok := typeNameValue.(string)
		if ok && typeName == "Ready" {
			return "active", false, nil
		}
	}
	return "pending", false, nil
}
