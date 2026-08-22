package ops

import (
	"fmt"
	"net/http"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
)

func registerPprof(mux *http.ServeMux) {
	mux.HandleFunc("GET /debug/pprof/", runtimeProfile)
}

func runtimeProfile(writer http.ResponseWriter, request *http.Request) {
	name := strings.TrimPrefix(request.URL.Path, "/debug/pprof/")
	if name == "" {
		profiles := pprof.Profiles()
		sort.Slice(profiles, func(first, second int) bool {
			return profiles[first].Name() < profiles[second].Name()
		})
		writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		for _, profile := range profiles {
			_, _ = fmt.Fprintf(writer, "%s\t%d\n", profile.Name(), profile.Count())
		}
		return
	}
	if strings.Contains(name, "/") {
		http.NotFound(writer, request)
		return
	}

	profile := pprof.Lookup(name)
	if profile == nil {
		http.NotFound(writer, request)
		return
	}
	debug := 0
	if raw := request.URL.Query().Get("debug"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			http.Error(writer, "invalid debug value", http.StatusBadRequest)
			return
		}
		debug = parsed
	}
	writer.Header().Set("Content-Type", "application/octet-stream")
	if err := profile.WriteTo(writer, debug); err != nil {
		http.Error(writer, "profile unavailable", http.StatusInternalServerError)
	}
}
