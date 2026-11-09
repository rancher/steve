package certificates

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TransformCertificate sets metadata.state to the state shown in the UI.
func TransformCertificate(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	stateName, stateError, err := getCertificateState(obj)
	if err != nil {
		return obj, err
	}
	unstructured.SetNestedField(obj.Object, stateName, "metadata", "state", "name")
	unstructured.SetNestedField(obj.Object, stateError, "metadata", "state", "error")
	return obj, err
}

// expiringSoonWindow mirrors the UI: a certificate within this long of its
// notAfter time is considered to be expiring.
const expiringSoonWindow = 30 * 24 * time.Hour

// getCertificateState computes the state name and error flag the same way the
// cert-manager UI extension does, using the standard cert-manager conditions.
func getCertificateState(obj *unstructured.Unstructured) (string, bool, error) {
	conditions, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil {
		return "", false, err
	}
	if !found {
		return "", true, fmt.Errorf("failed to find status.conditions block in certificate %q", obj.GetName())
	}
	now := time.Now()

	// Expiry beats everything: a certificate can report Ready while its stored material is stale.
	notAfter, hasNotAfter := getTime(obj, "status", "notAfter")
	if hasNotAfter && notAfter.Before(now) {
		return "expired", true, nil
	}
	renewalTime, hasRenewal := getTime(obj, "status", "renewalTime")
	if (hasRenewal && renewalTime.Before(now)) || (hasNotAfter && notAfter.Before(now.Add(expiringSoonWindow))) {
		return "expiring", false, nil
	}
	if conditionStatus(conditions, "Issuing") == "True" {
		return "in-progress", false, nil
	}
	failedIssuanceAttempts, _, _ := unstructured.NestedInt64(obj.Object, "status", "failedIssuanceAttempts")
	ready := conditionStatus(conditions, "Ready")
	if ready == "False" || failedIssuanceAttempts > 0 {
		return "error", true, nil
	}
	if ready == "True" {
		return "active", false, nil
	}
	return "pending", false, nil
}

func getTime(obj *unstructured.Unstructured, fields ...string) (time.Time, bool) {
	val, found, err := unstructured.NestedString(obj.Object, fields...)
	if !found || err != nil {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, val)
	return t, err == nil
}

// conditionStatus returns the status of the first condition of the given type, or "".
func conditionStatus(conditions []interface{}, condType string) string {
	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if !ok || cond["type"] != condType {
			continue
		}
		status, _ := cond["status"].(string)
		return status
	}
	return ""
}
