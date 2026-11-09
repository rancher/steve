package certificates

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func cond(typ, status string) interface{} {
	return map[string]interface{}{"type": typ, "status": status}
}

func newCert(status map[string]interface{}) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "Certificate",
		"metadata":   map[string]interface{}{"name": "c", "namespace": "ns"},
		"status":     status,
	}}
}

func TestTransformCertificate(t *testing.T) {
	ts := func(d time.Duration) string { return time.Now().Add(d).Format(time.RFC3339) }
	day := 24 * time.Hour

	tests := []struct {
		name      string
		status    map[string]interface{}
		wantState string
		wantErr   bool
	}{
		{
			name:    "no conditions",
			status:  map[string]interface{}{},
			wantErr: true,
		},
		{
			name:      "ready",
			status:    map[string]interface{}{"conditions": []interface{}{cond("Ready", "True")}},
			wantState: "active",
		},
		{
			name: "ready, far from expiry",
			status: map[string]interface{}{
				"conditions":  []interface{}{cond("Ready", "True")},
				"notAfter":    ts(90 * day),
				"renewalTime": ts(60 * day),
			},
			wantState: "active",
		},
		{
			name: "notAfter within 30 days, renewalTime still in future (7d cert, 1d renewBefore)",
			status: map[string]interface{}{
				"conditions":  []interface{}{cond("Ready", "True")},
				"notAfter":    ts(7 * day),
				"renewalTime": ts(6 * day),
			},
			wantState: "expiring",
		},
		{
			name: "renewalTime passed",
			status: map[string]interface{}{
				"conditions":  []interface{}{cond("Ready", "True")},
				"notAfter":    ts(90 * day),
				"renewalTime": ts(-day),
			},
			wantState: "expiring",
		},
		{
			name: "expired beats ready",
			status: map[string]interface{}{
				"conditions": []interface{}{cond("Ready", "True")},
				"notAfter":   ts(-day),
			},
			wantState: "expired",
		},
		{
			name:      "issuing",
			status:    map[string]interface{}{"conditions": []interface{}{cond("Ready", "False"), cond("Issuing", "True")}},
			wantState: "in-progress",
		},
		{
			name:      "issuing false is ignored",
			status:    map[string]interface{}{"conditions": []interface{}{cond("Ready", "True"), cond("Issuing", "False")}},
			wantState: "active",
		},
		{
			name:      "ready false",
			status:    map[string]interface{}{"conditions": []interface{}{cond("Ready", "False")}},
			wantState: "error",
		},
		{
			name: "failed issuance attempts",
			status: map[string]interface{}{
				"conditions":             []interface{}{cond("Ready", "True")},
				"failedIssuanceAttempts": int64(3),
			},
			wantState: "error",
		},
		{
			name:      "no ready condition",
			status:    map[string]interface{}{"conditions": []interface{}{cond("Other", "True")}},
			wantState: "pending",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out, err := TransformCertificate(newCert(test.status))
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			name, _, _ := unstructured.NestedString(out.Object, "metadata", "state", "name")
			isErr, _, _ := unstructured.NestedBool(out.Object, "metadata", "state", "error")
			require.Equal(t, test.wantState, name)
			require.Equal(t, test.wantState == "error" || test.wantState == "expired", isErr)
		})
	}
}
