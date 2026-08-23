package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/keelab/keelith/config"
)

const maxDiagnosticStringBytes = 4 * 1024

// AccessPolicy authorizes one optional diagnostic request.
//
// Returned errors are never exposed to the caller.
type AccessPolicy func(*http.Request) error

// BuildInfo is the bounded application build identity exposed by Ops.
type BuildInfo struct {
	Module    string `json:"module,omitempty"`
	Version   string `json:"version,omitempty"`
	GoVersion string `json:"goVersion,omitempty"`
	VCS       string `json:"vcs,omitempty"`
	Revision  string `json:"revision,omitempty"`
	BuildTime string `json:"buildTime,omitempty"`
	Modified  bool   `json:"modified"`
}

// ConfigStatus is a value-free snapshot of configuration runtime health.
type ConfigStatus struct {
	Loaded            bool   `json:"loaded"`
	Revision          string `json:"revision,omitempty"`
	Subscribers       int    `json:"subscribers"`
	FailedSubscribers int    `json:"failedSubscribers"`
	RestartRequired   int    `json:"restartRequired"`
	Rejected          bool   `json:"rejected"`
}

// ConfigStatusProvider obtains one bounded configuration diagnostic snapshot.
type ConfigStatusProvider func(context.Context) (ConfigStatus, error)

// WithAccessPolicy protects every enabled optional diagnostic endpoint.
func WithAccessPolicy(policy AccessPolicy) Option {
	return optionFunc(func(options *options) error {
		if policy == nil {
			return fmt.Errorf("access policy is nil")
		}
		options.accessPolicy = policy
		return nil
	})
}

// WithBuildInfo explicitly exposes info at GET /debug/build.
func WithBuildInfo(info BuildInfo) Option {
	return optionFunc(func(options *options) error {
		if err := validateBuildInfo(info); err != nil {
			return err
		}
		snapshot := info
		options.buildInfo = &snapshot
		return nil
	})
}

// WithConfigStatus exposes a value-free provider at GET /debug/config.
func WithConfigStatus(provider ConfigStatusProvider) Option {
	return optionFunc(func(options *options) error {
		if provider == nil {
			return fmt.Errorf("config status provider is nil")
		}
		options.configStatus = provider
		return nil
	})
}

// CurrentBuildInfo returns bounded Go module and VCS settings for this binary.
func CurrentBuildInfo() BuildInfo {
	result := BuildInfo{GoVersion: runtime.Version()}
	build, ok := debug.ReadBuildInfo()
	if !ok {
		return result
	}
	result.Module = build.Main.Path
	result.Version = build.Main.Version
	if build.GoVersion != "" {
		result.GoVersion = build.GoVersion
	}
	for _, setting := range build.Settings {
		switch setting.Key {
		case "vcs":
			result.VCS = setting.Value
		case "vcs.revision":
			result.Revision = setting.Value
		case "vcs.time":
			result.BuildTime = setting.Value
		case "vcs.modified":
			result.Modified, _ = strconv.ParseBool(setting.Value)
		}
	}
	return result
}

// ConfigManagerStatus adapts Manager diagnostics without exposing values or
// error text.
func ConfigManagerStatus(manager *config.Manager) ConfigStatusProvider {
	if manager == nil {
		return nil
	}
	return func(ctx context.Context) (ConfigStatus, error) {
		if ctx == nil {
			return ConfigStatus{}, fmt.Errorf("ops: config status context is nil")
		}
		if cause := context.Cause(ctx); cause != nil {
			return ConfigStatus{}, cause
		}
		snapshot, loaded := manager.Current()
		statuses := manager.SubscriberStatuses()
		failed := 0
		restartRequired := 0
		for _, status := range statuses {
			if status.LastError != "" {
				failed++
			}
			if status.RestartRequired {
				restartRequired++
			}
		}
		result := ConfigStatus{
			Loaded:            loaded,
			Subscribers:       len(statuses),
			FailedSubscribers: failed,
			RestartRequired:   restartRequired,
			Rejected:          manager.LastRejected() != "",
		}
		if loaded {
			result.Revision = snapshot.Revision()
		}
		return result, nil
	}
}

func validateBuildInfo(info BuildInfo) error {
	if info.Module == "" &&
		info.Version == "" &&
		info.GoVersion == "" &&
		info.Revision == "" {
		return fmt.Errorf("build identity is empty")
	}
	fields := []struct {
		name  string
		value string
	}{
		{name: "module", value: info.Module},
		{name: "version", value: info.Version},
		{name: "Go version", value: info.GoVersion},
		{name: "VCS", value: info.VCS},
		{name: "revision", value: info.Revision},
		{name: "build time", value: info.BuildTime},
	}
	for _, field := range fields {
		if !validDiagnosticString(field.value) {
			return fmt.Errorf("%s is malformed or too large", field.name)
		}
	}
	if info.BuildTime != "" {
		if _, err := time.Parse(time.RFC3339, info.BuildTime); err != nil {
			return fmt.Errorf("build time is not RFC3339")
		}
	}
	return nil
}

func validDiagnosticString(value string) bool {
	if len(value) > maxDiagnosticStringBytes || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func validateConfigStatus(status ConfigStatus) error {
	if status.Subscribers < 0 ||
		status.FailedSubscribers < 0 ||
		status.FailedSubscribers > status.Subscribers ||
		status.RestartRequired < 0 ||
		status.RestartRequired > status.FailedSubscribers {
		return fmt.Errorf("invalid subscriber counts")
	}
	if status.Loaded && status.Revision == "" {
		return fmt.Errorf("loaded config status has no revision")
	}
	if !status.Loaded && status.Revision != "" {
		return fmt.Errorf("unloaded config status has a revision")
	}
	if !validDiagnosticString(status.Revision) {
		return fmt.Errorf("config revision is malformed or too large")
	}
	return nil
}

func protectDiagnostic(
	policy AccessPolicy,
	handler http.Handler,
) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		setDiagnosticHeaders(writer.Header())
		if policy != nil {
			if err := authorizeDiagnostic(policy, request); err != nil {
				writeDiagnosticError(writer, http.StatusForbidden, "forbidden")
				return
			}
		}
		handler.ServeHTTP(writer, request)
	})
}

func buildInfoHandler(info BuildInfo) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeDiagnosticJSON(writer, http.StatusOK, info)
	})
}

func configStatusHandler(provider ConfigStatusProvider) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		status, err := readConfigStatus(request.Context(), provider)
		if err != nil || validateConfigStatus(status) != nil {
			writeDiagnosticError(
				writer,
				http.StatusServiceUnavailable,
				"unavailable",
			)
			return
		}
		writeDiagnosticJSON(writer, http.StatusOK, status)
	})
}

func authorizeDiagnostic(policy AccessPolicy, request *http.Request) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("ops: access policy panic")
		}
	}()
	return policy(request)
}

func readConfigStatus(ctx context.Context, provider ConfigStatusProvider) (status ConfigStatus, err error) {
	defer func() {
		if recover() != nil {
			status = ConfigStatus{}
			err = fmt.Errorf("ops: config status provider panic")
		}
	}()
	return provider(ctx)
}

func writeDiagnosticError(writer http.ResponseWriter, status int, value string) {
	writeDiagnosticJSON(writer, status, struct {
		Status string `json:"status"`
	}{Status: value})
}

func writeDiagnosticJSON(writer http.ResponseWriter, status int, value any) {
	setDiagnosticHeaders(writer.Header())
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func setDiagnosticHeaders(header http.Header) {
	header.Set("Cache-Control", "no-store")
	header.Set("X-Content-Type-Options", "nosniff")
}
