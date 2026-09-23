package informer

import (
	"regexp"
	"testing"
)

// isSimpleName replaced this regexp, and its contract is to stay identical to
// it. Keep the pattern here so the equivalence is checked rather than asserted.
var referenceSimpleName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// TestIsSimpleNameExhaustiveShort checks every 1- and 2-byte string, which
// covers the index-0 boundary: digits are rejected in the first position and
// accepted in every other one.
func TestIsSimpleNameExhaustiveShort(t *testing.T) {
	buf := make([]byte, 2)
	for a := 0; a < 256; a++ {
		buf[0] = byte(a)

		one := string(buf[:1])
		if got, want := isSimpleName(one), referenceSimpleName.MatchString(one); got != want {
			t.Fatalf("isSimpleName(%q) = %v, want %v", one, got, want)
		}

		for b := 0; b < 256; b++ {
			buf[1] = byte(b)

			two := string(buf[:2])
			if got, want := isSimpleName(two), referenceSimpleName.MatchString(two); got != want {
				t.Fatalf("isSimpleName(%q) = %v, want %v", two, got, want)
			}
		}
	}
}

func FuzzIsSimpleName(f *testing.F) {
	for _, s := range []string{
		"", "_", "a", "A", "0", "9", "_0", "0a", "a0", "aA_0",
		"metadata", "namespace", "test.cattle.io/summary",
		"a-b", "a b", "[a]", "n\x00me", "é", "\xff", "Z9z",
	} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, s string) {
		if got, want := isSimpleName(s), referenceSimpleName.MatchString(s); got != want {
			t.Fatalf("isSimpleName(%q) = %v, want %v", s, got, want)
		}
	})
}
