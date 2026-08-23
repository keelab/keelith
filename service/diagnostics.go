package service

import (
	"encoding/json"
	"net/http"
)

// DiagnosticHandler returns a bounded, immutable Profile and Surface topology
// projection. Access control remains the responsibility of the Ops listener.
func DiagnosticHandler(profile *Profile, surfaces ...*Surface) http.Handler {
	registry, err := NewSurfaceRegistry(surfaces...)
	if err != nil {
		registry = nil
	}
	return diagnosticHandler(profile, registry)
}

// DiagnosticHandlerFromRegistry returns an atomic topology projection from a
// previously validated runtime-wide listener registry.
func DiagnosticHandlerFromRegistry(profile *Profile, registry *SurfaceRegistry) http.Handler {
	return diagnosticHandler(profile, registry)
}

func diagnosticHandler(profile *Profile, registry *SurfaceRegistry) http.Handler {
	type snapshot struct {
		Profile  Description          `json:"profile"`
		Surfaces []SurfaceDescription `json:"surfaces"`
	}
	description := Description{}
	if profile != nil {
		description = profile.Describe()
	}
	surfaceDescriptions := registry.Describe()
	value := snapshot{Profile: description, Surfaces: surfaceDescriptions}
	return http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Cache-Control", "no-store")
		if err := json.NewEncoder(writer).Encode(value); err != nil {
			return
		}
	})
}
