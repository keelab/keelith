// Package version reports the Keelith build version.
package version

// value is replaced in release builds with:
//
//	-ldflags "-X github.com/keelab/keelith/internal/version.value=vX.Y.Z"
var value = "devel"

// String returns the Keelith build version.
func String() string {
	return value
}
