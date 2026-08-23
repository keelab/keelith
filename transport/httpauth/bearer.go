// Package httpauth provides fail-closed outbound HTTP authentication
// transports without coupling callers to plaintext credential material.
package httpauth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"
)

const maximumAuthorizationBytes = 64 * 1024

var (
	// ErrInvalidOption reports a nil transport, credential, or request.
	ErrInvalidOption = errors.New("http auth: invalid option")
	// ErrInsecureTransport prevents credentials from crossing plaintext HTTP.
	ErrInsecureTransport = errors.New("http auth: transport security required")
	// ErrAuthorizationConflict rejects caller-owned Authorization headers.
	ErrAuthorizationConflict = errors.New("http auth: authorization header conflict")
	// ErrCredentialUnavailable hides credential provider details from callers.
	ErrCredentialUnavailable = errors.New("http auth: credential unavailable")
)

// BearerCredential is the material-free subset implemented by Keelith's
// Secret-backed bearer lifecycle.
type BearerCredential interface {
	GetRequestMetadata(context.Context, ...string) (map[string]string, error)
	RequireTransportSecurity() bool
}

// Transport injects one current bearer credential into an independent clone
// of every HTTPS request. It never mutates the caller-owned request or header.
type Transport struct {
	base       http.RoundTripper
	credential BearerCredential
}

// NewTransport validates and constructs a bearer-authenticated RoundTripper.
func NewTransport(
	base http.RoundTripper,
	credential BearerCredential,
) (*Transport, error) {
	if base == nil || credential == nil ||
		!credential.RequireTransportSecurity() {
		return nil, ErrInvalidOption
	}
	return &Transport{base: base, credential: credential}, nil
}

// RoundTrip injects the latest valid token and delegates one HTTPS attempt.
func (t *Transport) RoundTrip(
	request *http.Request,
) (*http.Response, error) {
	if t == nil || t.base == nil ||
		t.credential == nil || request == nil || request.URL == nil {
		return nil, ErrInvalidOption
	}
	if !strings.EqualFold(request.URL.Scheme, "https") {
		return nil, ErrInsecureTransport
	}
	for name := range request.Header {
		if strings.EqualFold(name, "Authorization") {
			return nil, ErrAuthorizationConflict
		}
	}
	metadata, err := t.credential.GetRequestMetadata(request.Context())
	if err != nil {
		return nil, ErrCredentialUnavailable
	}
	authorization, err := authorizationValue(metadata)
	if err != nil {
		return nil, err
	}
	clone := request.Clone(request.Context())
	clone.Header.Set("Authorization", authorization)
	return t.base.RoundTrip(clone)
}

// CloseIdleConnections releases idle connections owned by the base transport.
func (t *Transport) CloseIdleConnections() {
	if t == nil || t.base == nil {
		return
	}
	if closer, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

func authorizationValue(metadata map[string]string) (string, error) {
	if len(metadata) != 1 {
		return "", ErrCredentialUnavailable
	}
	value := ""
	found := false
	for name, candidate := range metadata {
		if !strings.EqualFold(name, "authorization") || found {
			return "", ErrCredentialUnavailable
		}
		value = candidate
		found = true
	}
	if !found || len(value) > maximumAuthorizationBytes ||
		!utf8.ValidString(value) || !strings.HasPrefix(value, "Bearer ") ||
		strings.TrimSpace(strings.TrimPrefix(value, "Bearer ")) == "" {
		return "", ErrCredentialUnavailable
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return "", ErrCredentialUnavailable
		}
	}
	return value, nil
}

var _ http.RoundTripper = (*Transport)(nil)

func (t *Transport) String() string {
	if t == nil {
		return "httpauth.Transport<nil>"
	}
	return fmt.Sprintf("httpauth.Transport<configured=%t>", t.base != nil)
}
