package ops

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/keelab/keelith/health"
)

func TestRegisterPprofExposesStandardEndpoints(t *testing.T) {
	mux := http.NewServeMux()
	registerPprof(mux, nil)

	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "index", path: "/debug/pprof/", want: "text/html"},
		{name: "heap", path: "/debug/pprof/heap", want: "application/octet-stream"},
		{name: "cmdline", path: "/debug/pprof/cmdline", want: "text/plain"},
		{name: "symbol", path: "/debug/pprof/symbol", want: "text/plain"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, test.path, nil)
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)

			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
			}
			if contentType := response.Header().Get("Content-Type"); !strings.HasPrefix(contentType, test.want) {
				t.Fatalf("content type = %q, want prefix %q", contentType, test.want)
			}
			if test.name == "index" &&
				!strings.Contains(response.Body.String(), "<title>/debug/pprof/</title>") {
				t.Fatal("pprof index does not contain the standard title")
			}
		})
	}
}

func TestRegisterPprofCapturesCPUAndTrace(t *testing.T) {
	mux := http.NewServeMux()
	registerPprof(mux, nil)

	profileRequest := httptest.NewRequest(
		http.MethodGet,
		"/debug/pprof/profile?seconds=1",
		nil,
	)
	profileResponse := httptest.NewRecorder()
	mux.ServeHTTP(profileResponse, profileRequest)
	if profileResponse.Code != http.StatusOK {
		t.Fatalf("cpu profile status = %d, want %d", profileResponse.Code, http.StatusOK)
	}
	if profileResponse.Body.Len() == 0 {
		t.Fatal("cpu profile is empty")
	}

	traceRequest := httptest.NewRequest(
		http.MethodGet,
		"/debug/pprof/trace?seconds=0.01",
		nil,
	)
	traceResponse := httptest.NewRecorder()
	mux.ServeHTTP(traceResponse, traceRequest)
	if traceResponse.Code != http.StatusOK {
		t.Fatalf("trace status = %d, want %d", traceResponse.Code, http.StatusOK)
	}
	if traceResponse.Body.Len() == 0 {
		t.Fatal("trace is empty")
	}
}

func TestRegisterPprofUsesCustomCPUProfile(t *testing.T) {
	custom := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusAccepted)
	})
	mux := http.NewServeMux()
	registerPprof(mux, custom)

	request := httptest.NewRequest(
		http.MethodGet,
		"/debug/pprof/profile",
		nil,
	)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusAccepted)
	}
}

func TestServerPprofUsesOpsAccessPolicy(t *testing.T) {
	var policyCalls atomic.Int32
	server, err := New(
		health.NewRegistry(),
		WithAddress("127.0.0.1:0"),
		WithAccessPolicy(func(*http.Request) error {
			policyCalls.Add(1)
			return nil
		}),
		WithPprof(),
	)
	if err != nil {
		t.Fatalf("new ops server: %v", err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("start ops server: %v", err)
	}
	t.Cleanup(func() {
		stopContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Stop(stopContext); err != nil {
			t.Errorf("stop ops server: %v", err)
		}
	})

	address, ok := server.Address()
	if !ok {
		t.Fatal("ops server has no address after start")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Get("http://" + address + "/debug/pprof/")
	if err != nil {
		t.Fatalf("get pprof index: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	if !strings.HasPrefix(response.Header.Get("Content-Type"), "text/html") {
		t.Fatalf("content type = %q, want text/html", response.Header.Get("Content-Type"))
	}
	if policyCalls.Load() == 0 {
		t.Fatal("pprof request did not pass through the Ops access policy")
	}
}
