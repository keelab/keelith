package projection

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

const maxTenantIdentityBytes = 256

var (
	// ErrInvalidQuota reports malformed tenant identity or quota policy.
	ErrInvalidQuota = errors.New("projection: invalid quota")
	// ErrQuotaExceeded reports a bounded tenant resource rejection.
	ErrQuotaExceeded = errors.New("projection: quota exceeded")
	// ErrQuotaLeaseClosed reports accounting after a released lease.
	ErrQuotaLeaseClosed = errors.New("projection: quota lease closed")
)

// TenantClass is the only tenant label safe for metrics.
type TenantClass string

const (
	// TenantStandard is the default interactive subscriber class.
	TenantStandard TenantClass = "standard"
	// TenantBulk is for lower-priority snapshot and rebuild traffic.
	TenantBulk TenantClass = "bulk"
	// TenantSystem is for explicitly authorized platform replication.
	TenantSystem TenantClass = "system"
)

// QuotaDimension is a fixed-cardinality rejection reason.
type QuotaDimension string

const (
	// QuotaSessions limits concurrent subscriber sessions.
	QuotaSessions QuotaDimension = "sessions"
	// QuotaRows limits cumulative row mutations in one session.
	QuotaRows QuotaDimension = "rows"
	// QuotaFrames limits cumulative frames in one session.
	QuotaFrames QuotaDimension = "frames"
	// QuotaFrameBytes limits one encoded frame.
	QuotaFrameBytes QuotaDimension = "frame_bytes"
	// QuotaBytes limits cumulative encoded bytes in one session.
	QuotaBytes QuotaDimension = "bytes"
	// QuotaBandwidth limits the token-bucket send rate.
	QuotaBandwidth QuotaDimension = "bandwidth"
	// QuotaDisk limits retained owner snapshot and changelog bytes.
	QuotaDisk QuotaDimension = "disk"
)

// Tenant is an opaque stable digest plus a bounded class. The original
// identity is intentionally not recoverable or printable.
type Tenant struct {
	key   [sha256.Size]byte
	class TenantClass
}

// NewTenant validates and irreversibly hashes one authorized tenant identity.
func NewTenant(identity string, class TenantClass) (Tenant, error) {
	if !validIdentity(identity, maxTenantIdentityBytes) || !validTenantClass(class) {
		return Tenant{}, ErrInvalidQuota
	}
	return Tenant{key: sha256.Sum256([]byte(identity)), class: class}, nil
}

// Class returns the bounded metric-safe tenant class.
func (tenant Tenant) Class() TenantClass { return tenant.class }

// Valid reports whether tenant was created by NewTenant.
func (tenant Tenant) Valid() bool {
	return tenant.key != [sha256.Size]byte{} && validTenantClass(tenant.class)
}

// TenantLimits bounds all resources owned by one tenant digest.
type TenantLimits struct {
	MaxSessions          int64
	MaxRowsPerSession    int64
	MaxFramesPerSession  int64
	MaxFrameBytes        int64
	MaxBytesPerSession   int64
	BandwidthBytesPerSec int64
	BandwidthBurstBytes  int64
	MaxDiskBytes         int64
	RetryAfter           time.Duration
}

// QuotaPolicy configures limits by bounded tenant class.
type QuotaPolicy map[TenantClass]TenantLimits

// QuotaExceededError carries a typed retry delay without tenant identity.
type QuotaExceededError struct {
	Dimension  QuotaDimension
	Class      TenantClass
	RetryAfter time.Duration
}

// Error implements error without exposing tenant or projection payloads.
func (exceeded *QuotaExceededError) Error() string {
	if exceeded == nil {
		return ErrQuotaExceeded.Error()
	}
	return fmt.Sprintf(
		"%s: class %s dimension %s",
		ErrQuotaExceeded,
		exceeded.Class,
		exceeded.Dimension,
	)
}

// Unwrap supports errors.Is with ErrQuotaExceeded.
func (*QuotaExceededError) Unwrap() error { return ErrQuotaExceeded }

// QuotaManager owns shared session and disk accounting.
type QuotaManager struct {
	mu     sync.Mutex
	limits map[TenantClass]TenantLimits
	usage  map[[sha256.Size]byte]*tenantUsage
}

type tenantUsage struct {
	class    TenantClass
	sessions int64
	disk     int64
}

// QuotaClassUsage is a metric-safe aggregate with no tenant identity.
type QuotaClassUsage struct {
	Class          TenantClass
	ActiveTenants  int64
	ActiveSessions int64
	DiskBytes      int64
}

// NewQuotaManager validates and snapshots a class policy.
func NewQuotaManager(policy QuotaPolicy) (*QuotaManager, error) {
	if len(policy) == 0 || len(policy) > 3 {
		return nil, ErrInvalidQuota
	}
	limits := make(map[TenantClass]TenantLimits, len(policy))
	for class, configured := range policy {
		if !validTenantClass(class) || !validTenantLimits(configured) {
			return nil, fmt.Errorf("%w: class %q", ErrInvalidQuota, class)
		}
		limits[class] = configured
	}
	if _, exists := limits[TenantStandard]; !exists {
		return nil, fmt.Errorf("%w: standard class is required", ErrInvalidQuota)
	}
	return &QuotaManager{
		limits: limits,
		usage:  make(map[[sha256.Size]byte]*tenantUsage),
	}, nil
}

// AcquireSession reserves one tenant session and initializes its bandwidth
// bucket. Close must be called exactly once; repeated calls are harmless.
func (manager *QuotaManager) AcquireSession(
	tenant Tenant,
	now time.Time,
) (*SessionQuotaLease, error) {
	if manager == nil || !tenant.Valid() || now.IsZero() {
		return nil, ErrInvalidQuota
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	limits, exists := manager.limits[tenant.class]
	if !exists {
		return nil, ErrInvalidQuota
	}
	usage := manager.usage[tenant.key]
	if usage == nil {
		usage = &tenantUsage{class: tenant.class}
		manager.usage[tenant.key] = usage
	}
	if usage.sessions >= limits.MaxSessions {
		return nil, quotaError(QuotaSessions, tenant.class, limits.RetryAfter)
	}
	usage.sessions++
	return &SessionQuotaLease{
		manager: manager,
		tenant:  tenant,
		limits:  limits,
		last:    now,
		tokens:  float64(limits.BandwidthBurstBytes),
	}, nil
}

// SessionQuotaLease accounts cumulative rows, frames, bytes, and bandwidth.
type SessionQuotaLease struct {
	mu       sync.Mutex
	manager  *QuotaManager
	tenant   Tenant
	limits   TenantLimits
	rows     int64
	frames   int64
	bytes    int64
	last     time.Time
	tokens   float64
	released bool
}

// AllowFrame atomically charges one frame or returns a typed retryable error.
func (lease *SessionQuotaLease) AllowFrame(
	rows int,
	bytes int,
	now time.Time,
) error {
	if lease == nil || rows < 0 || bytes < 0 || now.IsZero() {
		return ErrInvalidQuota
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.released {
		return ErrQuotaLeaseClosed
	}
	rowCount := int64(rows)
	byteCount := int64(bytes)
	limits := lease.limits
	if byteCount > limits.MaxFrameBytes {
		return quotaError(QuotaFrameBytes, lease.tenant.class, limits.RetryAfter)
	}
	if lease.rows > limits.MaxRowsPerSession-rowCount {
		return quotaError(QuotaRows, lease.tenant.class, limits.RetryAfter)
	}
	if lease.frames >= limits.MaxFramesPerSession {
		return quotaError(QuotaFrames, lease.tenant.class, limits.RetryAfter)
	}
	if lease.bytes > limits.MaxBytesPerSession-byteCount {
		return quotaError(QuotaBytes, lease.tenant.class, limits.RetryAfter)
	}
	elapsed := now.Sub(lease.last)
	if elapsed > 0 {
		lease.tokens = math.Min(
			float64(limits.BandwidthBurstBytes),
			lease.tokens+elapsed.Seconds()*float64(limits.BandwidthBytesPerSec),
		)
		lease.last = now
	}
	if float64(byteCount) > lease.tokens {
		missing := float64(byteCount) - lease.tokens
		retry := time.Duration(
			math.Ceil(missing / float64(limits.BandwidthBytesPerSec) * float64(time.Second)),
		)
		if retry < limits.RetryAfter {
			retry = limits.RetryAfter
		}
		return quotaError(QuotaBandwidth, lease.tenant.class, retry)
	}
	lease.tokens -= float64(byteCount)
	lease.rows += rowCount
	lease.frames++
	lease.bytes += byteCount
	return nil
}

// Close releases the session token immediately and idempotently.
func (lease *SessionQuotaLease) Close() error {
	if lease == nil {
		return nil
	}
	lease.mu.Lock()
	if lease.released {
		lease.mu.Unlock()
		return nil
	}
	lease.released = true
	manager := lease.manager
	tenant := lease.tenant
	lease.mu.Unlock()
	if manager == nil {
		return nil
	}
	manager.mu.Lock()
	usage := manager.usage[tenant.key]
	if usage != nil {
		if usage.sessions > 0 {
			usage.sessions--
		}
		if usage.sessions == 0 && usage.disk == 0 {
			delete(manager.usage, tenant.key)
		}
	}
	manager.mu.Unlock()
	return nil
}

// DiskQuotaLease reserves owner-side retained snapshot/changelog capacity.
type DiskQuotaLease struct {
	manager  *QuotaManager
	tenant   Tenant
	bytes    int64
	released sync.Once
}

// ReserveDisk atomically charges retained bytes for one tenant.
func (manager *QuotaManager) ReserveDisk(
	tenant Tenant,
	bytes int64,
) (*DiskQuotaLease, error) {
	if manager == nil || !tenant.Valid() || bytes <= 0 {
		return nil, ErrInvalidQuota
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	limits, exists := manager.limits[tenant.class]
	if !exists {
		return nil, ErrInvalidQuota
	}
	usage := manager.usage[tenant.key]
	if usage == nil {
		usage = &tenantUsage{class: tenant.class}
		manager.usage[tenant.key] = usage
	}
	if usage.disk > limits.MaxDiskBytes-bytes {
		return nil, quotaError(QuotaDisk, tenant.class, limits.RetryAfter)
	}
	usage.disk += bytes
	return &DiskQuotaLease{manager: manager, tenant: tenant, bytes: bytes}, nil
}

// Close releases retained disk accounting idempotently.
func (lease *DiskQuotaLease) Close() error {
	if lease == nil {
		return nil
	}
	lease.released.Do(func() {
		manager := lease.manager
		if manager == nil {
			return
		}
		manager.mu.Lock()
		usage := manager.usage[lease.tenant.key]
		if usage != nil {
			usage.disk -= lease.bytes
			if usage.sessions == 0 && usage.disk == 0 {
				delete(manager.usage, lease.tenant.key)
			}
		}
		manager.mu.Unlock()
	})
	return nil
}

// ActiveTenants returns the number of opaque accounting buckets.
func (manager *QuotaManager) ActiveTenants() int {
	if manager == nil {
		return 0
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return len(manager.usage)
}

// SnapshotByClass returns fixed-cardinality usage suitable for metrics.
func (manager *QuotaManager) SnapshotByClass() []QuotaClassUsage {
	if manager == nil {
		return nil
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	byClass := make(map[TenantClass]QuotaClassUsage, len(manager.limits))
	for _, usage := range manager.usage {
		current := byClass[usage.class]
		current.Class = usage.class
		current.ActiveTenants++
		current.ActiveSessions += usage.sessions
		current.DiskBytes += usage.disk
		byClass[usage.class] = current
	}
	result := make([]QuotaClassUsage, 0, len(byClass))
	for _, class := range []TenantClass{TenantStandard, TenantBulk, TenantSystem} {
		if usage, exists := byClass[class]; exists {
			result = append(result, usage)
		}
	}
	return result
}

func validTenantClass(class TenantClass) bool {
	switch class {
	case TenantStandard, TenantBulk, TenantSystem:
		return true
	default:
		return false
	}
}

func validTenantLimits(limits TenantLimits) bool {
	return limits.MaxSessions > 0 && limits.MaxSessions <= 1_000_000 &&
		limits.MaxRowsPerSession > 0 && limits.MaxRowsPerSession <= 1_000_000_000 &&
		limits.MaxFramesPerSession > 0 && limits.MaxFramesPerSession <= 100_000_000 &&
		limits.MaxFrameBytes >= 256 && limits.MaxFrameBytes <= maxDeltaBytes &&
		limits.MaxBytesPerSession >= limits.MaxFrameBytes &&
		limits.MaxBytesPerSession <= 1<<50 &&
		limits.BandwidthBytesPerSec > 0 && limits.BandwidthBytesPerSec <= 1<<40 &&
		limits.BandwidthBurstBytes >= limits.MaxFrameBytes &&
		limits.BandwidthBurstBytes <= 1<<40 &&
		limits.MaxDiskBytes > 0 && limits.MaxDiskBytes <= 1<<60 &&
		limits.RetryAfter > 0 && limits.RetryAfter <= time.Hour
}

func quotaError(
	dimension QuotaDimension,
	class TenantClass,
	retryAfter time.Duration,
) error {
	return &QuotaExceededError{
		Dimension:  dimension,
		Class:      class,
		RetryAfter: retryAfter,
	}
}
