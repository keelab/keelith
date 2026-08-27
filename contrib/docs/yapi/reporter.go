// Package yapi imports Keelith-generated OpenAPI json into an optional YApi
// project while keeping OpenAPI as the only contract source of truth.
package yapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/keelab/keelith/secret"
)

const importPath = "/api/open/import_data"

// Credentials resolves a YApi project token through an already bootstrapped
// Secret Manager. The reference is safe to retain in ordinary configuration.
type Credentials struct {
	Manager *secret.Manager
	Token   secret.Reference
}

// Result is a material-free receipt for a successful import.
type Result struct {
	Merge          MergeMode
	DocumentBytes  int
	DocumentSHA256 string
}

// Reporter submits bounded OpenAPI documents to one YApi server.
type Reporter struct {
	client      *http.Client
	credentials Credentials
	options     Options
	importurl   string
}

type importResponse struct {
	ErrCode *int `json:"errcode"`
}

type documentHeader struct {
	OpenAPI string          `json:"openapi"`
	Swagger string          `json:"swagger"`
	Paths   json.RawMessage `json:"paths"`
}

// New constructs a Reporter. The supplied client is copied and redirects are
// disabled so a project token cannot cross an endpoint boundary.
func New(
	client *http.Client,
	credentials Credentials,
	options Options,
) (*Reporter, error) {
	if client == nil || credentials.Manager == nil {
		return nil, fmt.Errorf(
			"%w: http client and credential manager are required",
			ErrInvalidOption,
		)
	}
	if _, err := secret.NewReference(
		credentials.Token.Provider(),
		credentials.Token.Key(),
	); err != nil {
		return nil, fmt.Errorf("%w: token reference", ErrInvalidOption)
	}
	normalized, err := NormalizeOptions(options)
	if err != nil {
		return nil, err
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(
		_ *http.Request,
		_ []*http.Request,
	) error {
		return http.ErrUseLastResponse
	}
	return &Reporter{
		client:      &clientCopy,
		credentials: credentials,
		options:     normalized,
		importurl:   normalized.Endpoint + importPath,
	}, nil
}

// Publish validates and imports one OpenAPI 3.0/3.1 or Swagger 2.0 json
// document. Credentials are resolved on every call so provider-side rotation
// takes effect without reconstructing Reporter.
func (reporter *Reporter) Publish(
	ctx context.Context,
	document []byte,
) (Result, error) {
	if reporter == nil || ctx == nil {
		return Result{}, fmt.Errorf(
			"%w: reporter or context is nil",
			ErrInvalidOption,
		)
	}
	if cause := context.Cause(ctx); cause != nil {
		return Result{}, cause
	}
	if len(document) > reporter.options.MaxDocumentBytes {
		return Result{}, ErrTooLarge
	}
	documentSnapshot := append([]byte(nil), document...)
	if err := validateDocument(documentSnapshot); err != nil {
		return Result{}, err
	}

	requestContext, cancel := context.WithTimeout(
		ctx,
		reporter.options.RequestTimeout,
	)
	defer cancel()
	tokenValue, err := reporter.credentials.Manager.Resolve(
		requestContext,
		reporter.credentials.Token,
	)
	if err != nil {
		return Result{}, fmt.Errorf("docs/yapi: resolve token: %w", err)
	}
	token := tokenValue.Bytes()
	defer clear(token)
	trimmedToken := secret.TrimLineBreaks(token)
	if !validToken(trimmedToken, reporter.options.MaxTokenBytes) {
		return Result{}, fmt.Errorf(
			"%w: token material is invalid",
			ErrInvalidOption,
		)
	}

	form := encodeImportForm(reporter.options.Merge, trimmedToken, documentSnapshot)
	defer clear(form)
	request, err := http.NewRequestWithContext(
		requestContext,
		http.MethodPost,
		reporter.importurl,
		bytes.NewReader(form),
	)
	if err != nil {
		return Result{}, fmt.Errorf("%w: construct request", ErrInvalidOption)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("User-Agent", "keelith-yapi/1")
	response, err := reporter.client.Do(request)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return Result{}, cause
		}
		if cause := context.Cause(requestContext); cause != nil {
			return Result{}, fmt.Errorf("%w: %w", ErrUnavailable, cause)
		}
		return Result{}, fmt.Errorf("%w: request failed", ErrUnavailable)
	}
	if response == nil || response.Body == nil {
		return Result{}, fmt.Errorf("%w: response is nil", ErrInvalidResponse)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(
		response.Body,
		int64(reporter.options.MaxResponseBytes)+1,
	))
	if err != nil {
		return Result{}, fmt.Errorf("%w: read response", ErrUnavailable)
	}
	defer clear(body)
	if len(body) > reporter.options.MaxResponseBytes {
		return Result{}, ErrTooLarge
	}
	if err := responseError(response.StatusCode, body); err != nil {
		return Result{}, err
	}
	digest := sha256.Sum256(documentSnapshot)
	return Result{
		Merge:          reporter.options.Merge,
		DocumentBytes:  len(documentSnapshot),
		DocumentSHA256: hex.EncodeToString(digest[:]),
	}, nil
}

func validateDocument(document []byte) error {
	if len(document) == 0 {
		return ErrInvalidDocument
	}
	var header documentHeader
	if err := json.Unmarshal(document, &header); err != nil ||
		len(header.Paths) == 0 ||
		bytes.Equal(bytes.TrimSpace(header.Paths), []byte("null")) {
		return ErrInvalidDocument
	}
	var paths map[string]json.RawMessage
	if err := json.Unmarshal(header.Paths, &paths); err != nil {
		return ErrInvalidDocument
	}
	switch {
	case header.Swagger == "2.0" && header.OpenAPI == "":
		return nil
	case header.Swagger == "" &&
		(strings.HasPrefix(header.OpenAPI, "3.0.") ||
			strings.HasPrefix(header.OpenAPI, "3.1.")):
		return nil
	default:
		return ErrInvalidDocument
	}
}

func validToken(token []byte, maxBytes int) bool {
	if len(token) == 0 || len(token) > maxBytes {
		return false
	}
	for _, character := range token {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func encodeImportForm(mode MergeMode, token, document []byte) []byte {
	form := make([]byte, 0, len(document)+len(token)+64)
	form = appendFormField(form, "type", []byte("swagger"), false)
	form = appendFormField(form, "merge", []byte(mode), true)
	form = appendFormField(form, "token", token, true)
	return appendFormField(form, "json", document, true)
}

func appendFormField(
	destination []byte,
	name string,
	value []byte,
	separator bool,
) []byte {
	if separator {
		destination = append(destination, '&')
	}
	destination = append(destination, name...)
	destination = append(destination, '=')
	return appendFormEscaped(destination, value)
}

func appendFormEscaped(destination, value []byte) []byte {
	const hexadecimal = "0123456789ABCDEF"
	for _, character := range value {
		switch {
		case character >= 'a' && character <= 'z',
			character >= 'A' && character <= 'Z',
			character >= '0' && character <= '9',
			character == '-', character == '_', character == '.', character == '~':
			destination = append(destination, character)
		case character == ' ':
			destination = append(destination, '+')
		default:
			destination = append(
				destination,
				'%',
				hexadecimal[character>>4],
				hexadecimal[character&0x0f],
			)
		}
	}
	return destination
}

func responseError(statusCode int, body []byte) error {
	switch statusCode {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusForbidden:
		return ErrUnauthorized
	case http.StatusTooManyRequests:
		return ErrUnavailable
	default:
		if statusCode >= 500 {
			return ErrUnavailable
		}
		if statusCode >= 300 && statusCode < 400 {
			return fmt.Errorf(
				"%w: redirect status %d",
				ErrInvalidResponse,
				statusCode,
			)
		}
		return fmt.Errorf("%w: http status %d", ErrRejected, statusCode)
	}
	var result importResponse
	if err := json.Unmarshal(body, &result); err != nil || result.ErrCode == nil {
		return ErrInvalidResponse
	}
	if *result.ErrCode == 0 {
		return nil
	}
	if *result.ErrCode == 42014 {
		return ErrUnauthorized
	}
	return fmt.Errorf("%w: YApi code %d", ErrRejected, *result.ErrCode)
}
