// Package xxl implements the XXL-JOB cross-language OpenAPI as a Keelith
// worker.Scheduler.
package xxl

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/keelab/keelith/metadata"
	"github.com/keelab/keelith/worker"
)

const (
	accessTokenHeader = "XXL-JOB-ACCESS-TOKEN"
	defaultAddress    = "127.0.0.1:0"
	defaultInterval   = 30 * time.Second
	maxRequestBytes   = int64(1 << 20)
)

var (
	// ErrInvalidOption reports invalid executor/admin configuration.
	ErrInvalidOption = errors.New("xxl-job: invalid option")
	// ErrNotRunning reports a lifecycle call before Schedule.
	ErrNotRunning = errors.New("xxl-job: executor is not running")
	// ErrUnauthorized reports an access-token mismatch.
	ErrUnauthorized = errors.New("xxl-job: unauthorized")
)

// State is the value-free executor lifecycle.
type State string

const (
	// StateNew means Schedule has not opened the executor.
	StateNew State = "new"
	// StateRunning means remote runs are accepted.
	StateRunning State = "running"
	// StateDraining means registration/listening stopped while jobs drain.
	StateDraining State = "draining"
	// StateStopped means resources have been closed normally.
	StateStopped State = "stopped"
	// StateFailed means a runtime callback, registration, or listener failed.
	StateFailed State = "failed"
)

// Description is a payload-, job-id-, and error-free executor snapshot.
type Description struct {
	State                State
	Accepting            bool
	Registered           bool
	Active               int
	RunsAccepted         uint64
	RunsRejected         uint64
	Kills                uint64
	CallbackAttempts     uint64
	CallbackFailures     uint64
	RegistrationAttempts uint64
	RegistrationFailures uint64
	Unauthorized         uint64
	Capabilities         worker.SchedulerCapabilities
}

// Config controls the embedded executor and admin registration.
type Config struct {
	Address          string
	Publicurl        string
	Adminurl         string
	AppName          string
	HandlerName      string
	AccessToken      string
	RegisterInterval time.Duration
	HTTPClient       *http.Client
}

// Executor implements worker.Scheduler and the XXL-JOB executor OpenAPI.
type Executor struct {
	config Config
	client *http.Client

	mu         sync.Mutex
	handler    worker.JobHandler
	accepting  bool
	listener   net.Listener
	server     *http.Server
	address    string
	publicurl  string
	runErr     error
	closeErr   error
	cancelRun  context.CancelFunc
	registered bool
	closed     bool

	activeMu sync.Mutex
	active   map[int64]map[int64]context.CancelCauseFunc
	jobLocks map[int64]*sync.Mutex
	inflight sync.WaitGroup

	done      chan struct{}
	doneOnce  sync.Once
	closeOnce sync.Once

	runsAccepted         atomic.Uint64
	runsRejected         atomic.Uint64
	kills                atomic.Uint64
	callbackAttempts     atomic.Uint64
	callbackFailures     atomic.Uint64
	registrationAttempts atomic.Uint64
	registrationFailures atomic.Uint64
	unauthorized         atomic.Uint64
}

// New validates configuration without opening network resources.
func New(config Config) (*Executor, error) {
	config.Address = strings.TrimSpace(config.Address)
	if config.Address == "" {
		config.Address = defaultAddress
	}
	if _, _, err := net.SplitHostPort(config.Address); err != nil {
		return nil, fmt.Errorf("%w: address: %w", ErrInvalidOption, err)
	}
	config.AppName = strings.TrimSpace(config.AppName)
	config.HandlerName = strings.TrimSpace(config.HandlerName)
	if config.AppName == "" || config.HandlerName == "" {
		return nil, fmt.Errorf(
			"%w: app name and handler name are required",
			ErrInvalidOption,
		)
	}
	config.Publicurl = strings.TrimRight(strings.TrimSpace(config.Publicurl), "/")
	config.Adminurl = strings.TrimRight(strings.TrimSpace(config.Adminurl), "/")
	if config.Publicurl != "" {
		if err := validateHTTPurl(config.Publicurl); err != nil {
			return nil, fmt.Errorf("%w: public url: %w", ErrInvalidOption, err)
		}
	}
	if config.Adminurl != "" {
		if err := validateHTTPurl(config.Adminurl); err != nil {
			return nil, fmt.Errorf("%w: admin url: %w", ErrInvalidOption, err)
		}
		if config.Publicurl == "" {
			return nil, fmt.Errorf(
				"%w: public url is required with admin url",
				ErrInvalidOption,
			)
		}
	}
	if config.RegisterInterval == 0 {
		config.RegisterInterval = defaultInterval
	}
	if config.RegisterInterval < 0 {
		return nil, fmt.Errorf(
			"%w: register interval is negative",
			ErrInvalidOption,
		)
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &Executor{
		config:   config,
		client:   client,
		active:   make(map[int64]map[int64]context.CancelCauseFunc),
		jobLocks: make(map[int64]*sync.Mutex),
		done:     make(chan struct{}),
	}, nil
}

// SchedulerCapabilities declares remote authority, sharding, and kill support.
func (*Executor) SchedulerCapabilities() worker.SchedulerCapabilities {
	return worker.SchedulerCapabilities{
		TriggerAuthority: worker.TriggerAuthorityExternal,
		Ownership:        worker.OwnershipExternal,
		Sharding:         true,
		RemoteKill:       true,
	}
}

// Description returns bounded lifecycle and protocol counters.
func (executor *Executor) Description() Description {
	if executor == nil {
		return Description{
			State: StateStopped,
			Capabilities: worker.SchedulerCapabilities{
				TriggerAuthority: worker.TriggerAuthorityExternal,
				Ownership:        worker.OwnershipExternal,
				Sharding:         true,
				RemoteKill:       true,
			},
		}
	}
	executor.mu.Lock()
	state := StateNew
	switch {
	case executor.runErr != nil:
		state = StateFailed
	case executor.closed:
		state = StateStopped
	case executor.server != nil && executor.accepting:
		state = StateRunning
	case executor.server != nil:
		state = StateDraining
	}
	description := Description{
		State:        state,
		Accepting:    executor.accepting,
		Registered:   executor.registered,
		Capabilities: executor.SchedulerCapabilities(),
	}
	executor.mu.Unlock()
	executor.activeMu.Lock()
	for _, executions := range executor.active {
		description.Active += len(executions)
	}
	executor.activeMu.Unlock()
	description.RunsAccepted = executor.runsAccepted.Load()
	description.RunsRejected = executor.runsRejected.Load()
	description.Kills = executor.kills.Load()
	description.CallbackAttempts = executor.callbackAttempts.Load()
	description.CallbackFailures = executor.callbackFailures.Load()
	description.RegistrationAttempts = executor.registrationAttempts.Load()
	description.RegistrationFailures = executor.registrationFailures.Load()
	description.Unauthorized = executor.unauthorized.Load()
	return description
}

// Schedule starts the embedded executor, registers it, and returns ready.
func (executor *Executor) Schedule(
	ctx context.Context,
	handler worker.JobHandler,
) error {
	if executor == nil || handler == nil {
		return fmt.Errorf("%w: executor or handler is nil", ErrInvalidOption)
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	executor.mu.Lock()
	if executor.server != nil {
		executor.mu.Unlock()
		return fmt.Errorf("%w: already scheduled", ErrInvalidOption)
	}
	executor.mu.Unlock()

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", executor.config.Address)
	if err != nil {
		return fmt.Errorf("xxl-job: listen: %w", err)
	}
	publicurl := executor.config.Publicurl
	if publicurl == "" {
		publicurl = "http://" + listener.Addr().String()
	}
	runContext, cancelRun := context.WithCancel(context.Background())
	server := &http.Server{
		Handler:           executor.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	executor.mu.Lock()
	executor.handler = handler
	executor.listener = listener
	executor.server = server
	executor.address = listener.Addr().String()
	executor.publicurl = publicurl
	executor.accepting = true
	executor.cancelRun = cancelRun
	executor.mu.Unlock()
	go executor.serve(server, listener)

	if err := executor.registry(ctx, false); err != nil {
		executor.rollbackStart()
		return err
	}
	if executor.config.Adminurl != "" {
		go executor.registrationLoop(runContext)
	}
	return nil
}

// StopPulling stops registration and rejects new run requests before draining.
func (executor *Executor) StopPulling(ctx context.Context) error {
	if executor == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	executor.mu.Lock()
	if executor.server == nil {
		executor.mu.Unlock()
		return ErrNotRunning
	}
	executor.accepting = false
	cancelRun := executor.cancelRun
	server := executor.server
	executor.mu.Unlock()
	if cancelRun != nil {
		cancelRun()
	}
	removeErr := executor.registry(ctx, true)
	shutdownErr := server.Shutdown(ctx)
	if errors.Is(shutdownErr, http.ErrServerClosed) {
		shutdownErr = nil
	}
	return errors.Join(removeErr, shutdownErr)
}

// Drain waits for executions and result callbacks.
func (executor *Executor) Drain(ctx context.Context) error {
	if executor == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is nil", ErrInvalidOption)
	}
	drained := make(chan struct{})
	go func() {
		executor.inflight.Wait()
		close(drained)
	}()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		executor.cancelAll(context.Cause(ctx))
		return context.Cause(ctx)
	}
}

// Close releases the listener and unblocks Wait.
func (executor *Executor) Close(context.Context) error {
	if executor == nil {
		return nil
	}
	var result error
	executor.closeOnce.Do(func() {
		executor.mu.Lock()
		server := executor.server
		listener := executor.listener
		executor.mu.Unlock()
		if server != nil {
			if err := server.Close(); !errors.Is(err, http.ErrServerClosed) {
				result = errors.Join(result, err)
			}
		}
		if listener != nil {
			if err := listener.Close(); !errors.Is(err, net.ErrClosed) {
				result = errors.Join(result, err)
			}
		}
		executor.mu.Lock()
		executor.closeErr = result
		executor.closed = true
		executor.registered = false
		executor.mu.Unlock()
		executor.doneOnce.Do(func() { close(executor.done) })
	})
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.closeErr
}

// Wait reports embedded server, registration, or callback failure.
func (executor *Executor) Wait() error {
	if executor == nil {
		return nil
	}
	executor.mu.Lock()
	running := executor.server != nil
	executor.mu.Unlock()
	if !running {
		return ErrNotRunning
	}
	<-executor.done
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.runErr
}

// Address returns the actual embedded http listen address after Schedule.
func (executor *Executor) Address() (string, bool) {
	if executor == nil {
		return "", false
	}
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.address, executor.address != ""
}

func (executor *Executor) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /beat", executor.handleBeat)
	mux.HandleFunc("POST /idleBeat", executor.handleIdleBeat)
	mux.HandleFunc("POST /run", executor.handleRun)
	mux.HandleFunc("POST /kill", executor.handleKill)
	mux.HandleFunc("POST /log", executor.handleLog)
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !executor.authorized(request) {
			executor.unauthorized.Add(1)
			writeResponse(writer, http.StatusUnauthorized, apiResponse{
				Code: 500,
				Msg:  ErrUnauthorized.Error(),
			})
			return
		}
		mux.ServeHTTP(writer, request)
	})
}

func (executor *Executor) handleBeat(
	writer http.ResponseWriter,
	_ *http.Request,
) {
	writeResponse(writer, http.StatusOK, apiResponse{Code: 200})
}

func (executor *Executor) handleIdleBeat(
	writer http.ResponseWriter,
	request *http.Request,
) {
	var input idleBeatRequest
	if err := decodeRequest(writer, request, &input); err != nil {
		return
	}
	executor.activeMu.Lock()
	busy := len(executor.active[input.Jobid]) > 0
	executor.activeMu.Unlock()
	if busy {
		writeResponse(writer, http.StatusOK, apiResponse{
			Code: 500,
			Msg:  "job is running",
		})
		return
	}
	writeResponse(writer, http.StatusOK, apiResponse{Code: 200})
}

func (executor *Executor) handleRun(
	writer http.ResponseWriter,
	request *http.Request,
) {
	var input runRequest
	if err := decodeRequest(writer, request, &input); err != nil {
		return
	}
	executor.mu.Lock()
	accepting := executor.accepting
	handler := executor.handler
	executor.mu.Unlock()
	if !accepting {
		executor.runsRejected.Add(1)
		writeResponse(writer, http.StatusServiceUnavailable, apiResponse{
			Code: 500,
			Msg:  "executor is draining",
		})
		return
	}
	if input.ExecutorHandler != executor.config.HandlerName {
		executor.runsRejected.Add(1)
		writeResponse(writer, http.StatusOK, apiResponse{
			Code: 500,
			Msg:  "unknown executor handler",
		})
		return
	}
	executionContext, cancelExecution := context.WithCancelCause(
		context.Background(),
	)
	if !executor.prepareExecution(input, cancelExecution) {
		executor.runsRejected.Add(1)
		cancelExecution(errors.New("xxl-job: execution was rejected"))
		writeResponse(writer, http.StatusOK, apiResponse{
			Code: 500,
			Msg:  "job is already running",
		})
		return
	}
	executor.inflight.Add(1)
	executor.runsAccepted.Add(1)
	go executor.execute(
		executionContext,
		cancelExecution,
		input,
		handler,
	)
	writeResponse(writer, http.StatusOK, apiResponse{Code: 200})
}

func (executor *Executor) handleKill(
	writer http.ResponseWriter,
	request *http.Request,
) {
	var input killRequest
	if err := decodeRequest(writer, request, &input); err != nil {
		return
	}
	executor.cancelJob(input.Jobid, errors.New("xxl-job: killed by scheduler"))
	executor.kills.Add(1)
	writeResponse(writer, http.StatusOK, apiResponse{Code: 200})
}

func (executor *Executor) handleLog(
	writer http.ResponseWriter,
	request *http.Request,
) {
	var input logRequest
	if err := decodeRequest(writer, request, &input); err != nil {
		return
	}
	writeResponse(writer, http.StatusOK, apiResponse{
		Code: 200,
		Content: logResponse{
			FromLineNum: input.FromLineNum,
			ToLineNum:   input.FromLineNum,
			LogContent:  "Keelith delegates logs to the configured slog handler",
			IsEnd:       true,
		},
	})
}

func (executor *Executor) prepareExecution(
	input runRequest,
	cancelExecution context.CancelCauseFunc,
) bool {
	executor.activeMu.Lock()
	defer executor.activeMu.Unlock()
	active := executor.active[input.Jobid]
	switch strings.ToUpper(input.ExecutorBlockStrategy) {
	case "DISCARD_LATER":
		if len(active) > 0 {
			return false
		}
	case "COVER_EARLY":
		for _, cancel := range active {
			cancel(errors.New("xxl-job: covered by a newer execution"))
		}
	}
	if active == nil {
		active = make(map[int64]context.CancelCauseFunc)
		executor.active[input.Jobid] = active
	}
	active[input.Logid] = cancelExecution
	return true
}

func (executor *Executor) execute(
	ctx context.Context,
	cancel context.CancelCauseFunc,
	input runRequest,
	handler worker.JobHandler,
) {
	defer executor.inflight.Done()
	defer cancel(nil)
	if input.ExecutorTimeout > 0 {
		var timeoutCancel context.CancelFunc
		ctx, timeoutCancel = context.WithTimeout(
			ctx,
			time.Duration(input.ExecutorTimeout)*time.Second,
		)
		defer timeoutCancel()
	}
	executor.activeMu.Lock()
	lock := executor.jobLocks[input.Jobid]
	if lock == nil {
		lock = &sync.Mutex{}
		executor.jobLocks[input.Jobid] = lock
	}
	executor.activeMu.Unlock()
	defer executor.removeExecution(input.Jobid, input.Logid)

	if strings.ToUpper(input.ExecutorBlockStrategy) != "COVER_EARLY" &&
		strings.ToUpper(input.ExecutorBlockStrategy) != "DISCARD_LATER" {
		lock.Lock()
		defer lock.Unlock()
	}
	inbound, _ := metadata.New(map[string][]string{
		"xxl-job-id":              {strconv.FormatInt(input.Jobid, 10)},
		"xxl-job-log-id":          {strconv.FormatInt(input.Logid, 10)},
		"xxl-job-broadcast-index": {strconv.Itoa(input.BroadcastIndex)},
		"xxl-job-broadcast-total": {strconv.Itoa(input.BroadcastTotal)},
	})
	execution := worker.NewExecution(
		strconv.FormatInt(input.Logid, 10),
		time.UnixMilli(input.LogDateTime),
		[]byte(input.ExecutorParams),
		inbound,
	)
	result := handler(ctx, execution)
	if err := executor.callback(context.Background(), input, result); err != nil {
		executor.fail(fmt.Errorf("xxl-job: callback: %w", err))
	}
}

func (executor *Executor) callback(
	ctx context.Context,
	input runRequest,
	result worker.Result,
) error {
	if executor.config.Adminurl == "" {
		return nil
	}
	executor.callbackAttempts.Add(1)
	code := 200
	message := ""
	if result.Action() != worker.ActionAck {
		code = 500
		if result.Cause() != nil {
			message = result.Cause().Error()
		} else {
			message = string(result.Action())
		}
	}
	err := executor.post(
		ctx,
		"/api/callback",
		[]callbackRequest{{
			Logid:       input.Logid,
			LogDateTime: input.LogDateTime,
			HandleCode:  code,
			HandleMsg:   message,
		}},
	)
	if err != nil {
		executor.callbackFailures.Add(1)
	}
	return err
}

func (executor *Executor) registry(
	ctx context.Context,
	remove bool,
) error {
	if executor.config.Adminurl == "" {
		return nil
	}
	executor.registrationAttempts.Add(1)
	path := "/api/registry"
	if remove {
		path = "/api/registryRemove"
	}
	err := executor.post(ctx, path, registryRequest{
		RegistryGroup: "EXECUTOR",
		RegistryKey:   executor.config.AppName,
		RegistryValue: executor.publicurl + "/",
	})
	executor.mu.Lock()
	if err == nil {
		executor.registered = !remove
	}
	executor.mu.Unlock()
	if err != nil {
		executor.registrationFailures.Add(1)
	}
	return err
}

func (executor *Executor) post(
	ctx context.Context,
	path string,
	payload any,
) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		executor.config.Adminurl+path,
		strings.NewReader(string(body)),
	)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if executor.config.AccessToken != "" {
		request.Header.Set(accessTokenHeader, executor.config.AccessToken)
	}
	response, err := executor.client.Do(request)
	if err != nil {
		return err
	}
	defer func() {
		_ = response.Body.Close()
	}()
	content, err := io.ReadAll(io.LimitReader(response.Body, maxRequestBytes))
	if err != nil {
		return err
	}
	var decoded apiResponse
	if err := json.Unmarshal(content, &decoded); err != nil {
		return fmt.Errorf("decode admin response: %w", err)
	}
	if response.StatusCode >= http.StatusBadRequest || decoded.Code != 200 {
		return fmt.Errorf(
			"admin rejected request: http %d code %d: %s",
			response.StatusCode,
			decoded.Code,
			decoded.Msg,
		)
	}
	return nil
}

func (executor *Executor) registrationLoop(ctx context.Context) {
	ticker := time.NewTicker(executor.config.RegisterInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := executor.registry(ctx, false); err != nil {
				if context.Cause(ctx) == nil {
					executor.fail(fmt.Errorf("xxl-job: refresh registration: %w", err))
				}
				return
			}
		}
	}
}

func (executor *Executor) serve(
	server *http.Server,
	listener net.Listener,
) {
	err := server.Serve(listener)
	if !errors.Is(err, http.ErrServerClosed) {
		executor.fail(fmt.Errorf("xxl-job: serve: %w", err))
		return
	}
	executor.doneOnce.Do(func() { close(executor.done) })
}

func (executor *Executor) fail(err error) {
	executor.mu.Lock()
	if executor.runErr == nil {
		executor.runErr = err
	}
	server := executor.server
	executor.accepting = false
	executor.registered = false
	executor.mu.Unlock()
	if server != nil {
		_ = server.Close()
	}
	executor.doneOnce.Do(func() { close(executor.done) })
}

func (executor *Executor) rollbackStart() {
	executor.mu.Lock()
	server := executor.server
	executor.accepting = false
	executor.registered = false
	executor.mu.Unlock()
	if server != nil {
		_ = server.Close()
	}
	executor.doneOnce.Do(func() { close(executor.done) })
}

func (executor *Executor) authorized(request *http.Request) bool {
	want := executor.config.AccessToken
	if want == "" {
		return true
	}
	got := request.Header.Get(accessTokenHeader)
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (executor *Executor) removeExecution(jobid, logid int64) {
	executor.activeMu.Lock()
	defer executor.activeMu.Unlock()
	delete(executor.active[jobid], logid)
	if len(executor.active[jobid]) == 0 {
		delete(executor.active, jobid)
		delete(executor.jobLocks, jobid)
	}
}

func (executor *Executor) cancelJob(jobid int64, cause error) {
	executor.activeMu.Lock()
	cancellations := make([]context.CancelCauseFunc, 0, len(executor.active[jobid]))
	for _, cancel := range executor.active[jobid] {
		cancellations = append(cancellations, cancel)
	}
	executor.activeMu.Unlock()
	for _, cancel := range cancellations {
		cancel(cause)
	}
}

func (executor *Executor) cancelAll(cause error) {
	executor.activeMu.Lock()
	cancellations := make([]context.CancelCauseFunc, 0)
	for _, active := range executor.active {
		for _, cancel := range active {
			cancellations = append(cancellations, cancel)
		}
	}
	executor.activeMu.Unlock()
	for _, cancel := range cancellations {
		cancel(cause)
	}
}

func decodeRequest(
	writer http.ResponseWriter,
	request *http.Request,
	target any,
) error {
	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeResponse(writer, http.StatusBadRequest, apiResponse{
			Code: 500,
			Msg:  err.Error(),
		})
		return err
	}
	return nil
}

func writeResponse(
	writer http.ResponseWriter,
	status int,
	response apiResponse,
) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(response)
}

func validateHTTPurl(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil ||
		parsed.Scheme != "http" && parsed.Scheme != "https" ||
		parsed.Host == "" ||
		parsed.User != nil ||
		parsed.RawQuery != "" ||
		parsed.Fragment != "" {
		return errors.New("must be an http(s) origin without credentials or query")
	}
	return nil
}

type apiResponse struct {
	Code    int    `json:"code"`
	Msg     string `json:"msg,omitempty"`
	Content any    `json:"content,omitempty"`
}

type registryRequest struct {
	RegistryGroup string `json:"registryGroup"`
	RegistryKey   string `json:"registryKey"`
	RegistryValue string `json:"registryValue"`
}

type callbackRequest struct {
	Logid       int64  `json:"logId"`
	LogDateTime int64  `json:"logDateTim"`
	HandleCode  int    `json:"handleCode"`
	HandleMsg   string `json:"handleMsg,omitempty"`
}

type idleBeatRequest struct {
	Jobid int64 `json:"jobId"`
}

type killRequest struct {
	Jobid int64 `json:"jobId"`
}

type logRequest struct {
	LogDateTime int64 `json:"logDateTim"`
	Logid       int64 `json:"logId"`
	FromLineNum int   `json:"fromLineNum"`
}

type logResponse struct {
	FromLineNum int    `json:"fromLineNum"`
	ToLineNum   int    `json:"toLineNum"`
	LogContent  string `json:"logContent"`
	IsEnd       bool   `json:"isEnd"`
}

type runRequest struct {
	Jobid                 int64  `json:"jobId"`
	ExecutorHandler       string `json:"executorHandler"`
	ExecutorParams        string `json:"executorParams"`
	ExecutorBlockStrategy string `json:"executorBlockStrategy"`
	ExecutorTimeout       int    `json:"executorTimeout"`
	Logid                 int64  `json:"logId"`
	LogDateTime           int64  `json:"logDateTime"`
	GlueType              string `json:"glueType"`
	GlueSource            string `json:"glueSource"`
	GlueUpdateTime        int64  `json:"glueUpdatetime"`
	BroadcastIndex        int    `json:"broadcastIndex"`
	BroadcastTotal        int    `json:"broadcastTotal"`
}
