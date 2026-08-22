package env

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const (
	jsonValuePrefix   = "json:"
	maxJSONValueBytes = 1 * 1024 * 1024
)

// JSONValueParser preserves ordinary environment strings and strictly
// decodes values prefixed with "json:".
//
// The explicit prefix prevents service versions, numeric-looking identifiers,
// durations, and boolean-looking strings from changing type accidentally.
func JSONValueParser(_ []string, value string) (any, error) {
	if !strings.HasPrefix(value, jsonValuePrefix) {
		return value, nil
	}
	payload := strings.TrimPrefix(value, jsonValuePrefix)
	if payload == "" || len(payload) > maxJSONValueBytes {
		return nil, fmt.Errorf("%w: json value is empty or exceeds %d bytes", ErrInvalidOption, maxJSONValueBytes)
	}
	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("%w: invalid json value", ErrInvalidOption)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf(
			"%w: json value contains trailing data",
			ErrInvalidOption,
		)
	}
	return decoded, nil
}
