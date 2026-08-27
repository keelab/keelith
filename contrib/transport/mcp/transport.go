package mcp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"

	"github.com/keelab/keelith/operation"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

type callFacts struct {
	transport string
	peer      operation.Peer
	hasPeer   bool
}

type callFactsContextKey struct{}

// HTTPHandler returns a stateless Streamable http handler with explicit origin
// and message-size protection. Authentication remains the owner's responsibility.
func (server *Server) HTTPHandler(options HTTPOptions) (http.Handler, error) {
	if server == nil || server.sdk == nil {
		return nil, fmt.Errorf("%w: server is nil", ErrInvalidOption)
	}
	normalized, err := normalizeHTTPOptions(options)
	if err != nil {
		return nil, err
	}
	protection := http.NewCrossOriginProtection()
	for _, origin := range normalized.TrustedOrigins {
		if err := protection.AddTrustedOrigin(origin); err != nil {
			return nil, fmt.Errorf(
				"%w: trusted origin is invalid",
				ErrInvalidOption,
			)
		}
	}
	server.freeze()
	protocolHandler := sdk.NewStreamableHTTPHandler(
		func(*http.Request) *sdk.Server { return server.sdk },
		&sdk.StreamableHTTPOptions{
			Stateless:    true,
			JSONResponse: normalized.JSONResponse,
		},
	)
	bounded := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		serveBoundedHTTP(
			writer,
			request,
			server.maxMessageBytes,
			protocolHandler,
		)
	})
	return protection.Handler(bounded), nil
}

// RunStdio serves the frozen registry over process stdin/stdout until cancelled.
func (server *Server) RunStdio(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	if server == nil || server.sdk == nil {
		return fmt.Errorf("%w: server is nil", ErrInvalidOption)
	}
	server.mu.Lock()
	if server.stdioRunning {
		server.mu.Unlock()
		return fmt.Errorf("%w: stdio is already running", ErrInvalidOption)
	}
	server.stdioRunning = true
	server.frozen = true
	server.mu.Unlock()
	defer func() {
		server.mu.Lock()
		server.stdioRunning = false
		server.mu.Unlock()
	}()
	return server.runIO(ctx, os.Stdin, nopWriteCloser{Writer: os.Stdout})
}

// RunIO serves the stdio protocol over caller-owned streams. The SDK closes
// both streams when the session ends.
func (server *Server) RunIO(
	ctx context.Context,
	reader io.ReadCloser,
	writer io.WriteCloser,
) error {
	if ctx == nil {
		return ErrNilContext
	}
	if server == nil || server.sdk == nil {
		return fmt.Errorf("%w: server is nil", ErrInvalidOption)
	}
	if isNilStream(reader) || isNilStream(writer) {
		return ErrInvalidStream
	}
	server.freeze()
	return server.runIO(ctx, reader, writer)
}

func (server *Server) runIO(
	ctx context.Context,
	reader io.ReadCloser,
	writer io.WriteCloser,
) error {
	peer, _ := operation.NewPeer("stdio", "local")
	ctx = context.WithValue(ctx, callFactsContextKey{}, callFacts{
		transport: stdioTransport,
		peer:      peer,
		hasPeer:   true,
	})
	return server.sdk.Run(ctx, &sdk.IOTransport{
		Reader: newBoundedNDjsonReadCloser(reader, server.maxMessageBytes),
		Writer: writer,
	})
}

func (server *Server) freeze() {
	server.mu.Lock()
	server.frozen = true
	server.mu.Unlock()
}

func serveBoundedHTTP(
	writer http.ResponseWriter,
	request *http.Request,
	maxBytes int64,
	next http.Handler,
) {
	if request == nil {
		writeHTTPFailure(writer, http.StatusBadRequest, "invalid MCP request")
		return
	}
	if request.ContentLength > maxBytes {
		writeHTTPFailure(writer, http.StatusRequestEntityTooLarge, "MCP request is too large")
		return
	}
	if request.Body != nil && request.ContentLength != 0 {
		body, err := io.ReadAll(io.LimitReader(request.Body, maxBytes+1))
		_ = request.Body.Close()
		if err != nil {
			clear(body)
			writeHTTPFailure(writer, http.StatusBadRequest, "invalid MCP request")
			return
		}
		if int64(len(body)) > maxBytes {
			clear(body)
			writeHTTPFailure(writer, http.StatusRequestEntityTooLarge, "MCP request is too large")
			return
		}
		defer clear(body)
		request.Body = io.NopCloser(bytes.NewReader(body))
		request.ContentLength = int64(len(body))
	}
	facts := callFacts{transport: httpTransport}
	if request.RemoteAddr != "" {
		if peer, err := operation.NewPeer("tcp", request.RemoteAddr); err == nil {
			facts.peer = peer
			facts.hasPeer = true
		}
	}
	ctx := context.WithValue(request.Context(), callFactsContextKey{}, facts)
	next.ServeHTTP(writer, request.WithContext(ctx))
}

func writeHTTPFailure(writer http.ResponseWriter, status int, message string) {
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.WriteHeader(status)
	_, _ = io.WriteString(writer, message+"\n")
}

func callFactsFromContext(ctx context.Context) (callFacts, bool) {
	if ctx == nil {
		return callFacts{}, false
	}
	facts, ok := ctx.Value(callFactsContextKey{}).(callFacts)
	return facts, ok && facts.transport != ""
}

type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }

type boundedNDjsonReadCloser struct {
	reader   *bufio.Reader
	closer   io.Closer
	maxBytes int64

	mu      sync.Mutex
	pending []byte
	offset  int
	failed  error
}

func newBoundedNDjsonReadCloser(
	reader io.ReadCloser,
	maxBytes int64,
) *boundedNDjsonReadCloser {
	return &boundedNDjsonReadCloser{
		reader:   bufio.NewReaderSize(reader, 32*1024),
		closer:   reader,
		maxBytes: maxBytes,
	}
}

func (reader *boundedNDjsonReadCloser) Read(destination []byte) (int, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if len(destination) == 0 {
		return 0, nil
	}
	if reader.failed != nil {
		return 0, reader.failed
	}
	if reader.offset >= len(reader.pending) {
		line, err := reader.readLine()
		if err != nil {
			reader.failed = err
			return 0, err
		}
		reader.pending = line
		reader.offset = 0
	}
	copied := copy(destination, reader.pending[reader.offset:])
	reader.offset += copied
	if reader.offset == len(reader.pending) {
		clear(reader.pending)
		reader.pending = nil
		reader.offset = 0
	}
	return copied, nil
}

func (reader *boundedNDjsonReadCloser) readLine() ([]byte, error) {
	line := make([]byte, 0, 32*1024)
	for {
		fragment, err := reader.reader.ReadSlice('\n')
		line = append(line, fragment...)
		limit := reader.maxBytes
		if len(line) > 0 && line[len(line)-1] == '\n' {
			limit++
		}
		if int64(len(line)) > limit {
			clear(line)
			return nil, ErrMessageTooLarge
		}
		switch {
		case err == nil:
			return line, nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF) && len(line) > 0:
			return append(line, '\n'), nil
		default:
			clear(line)
			return nil, err
		}
	}
}

func (reader *boundedNDjsonReadCloser) Close() error {
	return reader.closer.Close()
}
