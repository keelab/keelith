package ops

import (
	"net/http"
	netpprof "net/http/pprof"
)

// registerPprof binds the standard library handlers to the private Ops mux.
// Importing net/http/pprof also registers the same handlers on
// http.DefaultServeMux as a package side effect; callers must not expose that
// global mux as an unauthenticated public server.
func registerPprof(mux *http.ServeMux, cpuProfile http.Handler) {
	mux.HandleFunc("GET /debug/pprof/", netpprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", netpprof.Cmdline)
	if cpuProfile == nil {
		mux.HandleFunc("GET /debug/pprof/profile", netpprof.Profile)
	} else {
		mux.Handle("GET /debug/pprof/profile", cpuProfile)
	}
	mux.HandleFunc("GET /debug/pprof/symbol", netpprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", netpprof.Trace)
}
