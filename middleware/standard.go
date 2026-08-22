package middleware

// ServerBundleConfig supplies optional standard server stages.
//
// The bundle always records final order through Describe. Recovery is enabled
// unless DisableRecovery is true.
type ServerBundleConfig struct {
	Source            string
	Observability     Middleware
	Policy            Middleware
	Authentication    Middleware
	Authorization     Middleware
	RateLimit         Middleware
	LoadShedding      Middleware
	Timeout           Middleware
	Validation        Middleware
	RecoveryReporter  PanicReporter
	DisableRecovery   bool
	AdditionalEntries []Entry
}

// NewServerBundle assembles the auditable default server middleware order.
func NewServerBundle(config ServerBundleConfig) (*Bundle, error) {
	source := config.Source
	if source == "" {
		source = "standard-default"
	}
	entries := make([]Entry, 0, 9+len(config.AdditionalEntries))
	appendStage := func(name string, stage Middleware) {
		if stage == nil {
			return
		}
		entries = append(entries, Entry{
			Name:       name,
			Source:     source,
			Middleware: stage,
		})
	}
	appendStage("observability", config.Observability)
	if !config.DisableRecovery {
		appendStage("recovery", Recovery(config.RecoveryReporter))
	}
	appendStage("policy-snapshot", config.Policy)
	appendStage("authentication", config.Authentication)
	appendStage("authorization", config.Authorization)
	appendStage("rate-limit", config.RateLimit)
	appendStage("load-shedding", config.LoadShedding)
	appendStage("timeout", config.Timeout)
	appendStage("validation", config.Validation)
	entries = append(entries, config.AdditionalEntries...)
	return NewBundle(entries...)
}
