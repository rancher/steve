package formatters

import (
	"github.com/rancher/steve/pkg/resources/virtual/multivalue/composite"
)

// FormatRestarts formats a restart count array as a display string.
// This is a thin wrapper over CompositeInt.FormatAsRestarts() for backward compatibility.
func FormatRestarts(values []interface{}) string {
	ci := composite.CompositeInt{}.From(values)
	return ci.FormatAsRestarts()
}
