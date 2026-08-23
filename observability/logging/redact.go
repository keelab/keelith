package logging

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"unicode"
)

const defaultReplacement = "[REDACTED]"

// ErrInvalidRedacter reports an empty or duplicate sensitive key.
var ErrInvalidRedacter = errors.New("logging: invalid redacter")

// Redacter replaces sensitive slog attributes, including composite key forms
// and nested groups.
type Redacter struct {
	keys        map[string]struct{}
	replacement string
}

// NewRedacter validates normalized sensitive keys.
func NewRedacter(keys ...string) (Redacter, error) {
	result := Redacter{
		keys:        make(map[string]struct{}, len(keys)),
		replacement: defaultReplacement,
	}
	for _, key := range keys {
		normalized := strings.ToLower(strings.TrimSpace(key))
		if normalized == "" {
			return Redacter{}, fmt.Errorf("%w: key is empty", ErrInvalidRedacter)
		}
		if _, duplicate := result.keys[normalized]; duplicate {
			return Redacter{}, fmt.Errorf(
				"%w: duplicate key %q",
				ErrInvalidRedacter,
				key,
			)
		}
		result.keys[normalized] = struct{}{}
	}
	return result, nil
}

// Redact returns an independent safe attribute without resolving user values.
// KindAny and KindLogValuer values are replaced rather than delegated.
func (redacter Redacter) Redact(attribute slog.Attr) slog.Attr {
	if redacter.sensitiveKey(attribute.Key) {
		return redacter.replacementAttr(attribute.Key)
	}
	switch attribute.Value.Kind() {
	case slog.KindAny:
		if attribute.Key == "" && attribute.Value.Any() == nil {
			return slog.Attr{}
		}
		return redacter.replacementAttr(attribute.Key)
	case slog.KindLogValuer:
		return redacter.replacementAttr(attribute.Key)
	case slog.KindGroup:
		return redacter.redactGroup(attribute)
	default:
		return attribute
	}
}

func (redacter Redacter) redactGroup(attribute slog.Attr) slog.Attr {
	group := attribute.Value.Group()
	safe := make([]slog.Attr, 0, len(group))
	for _, nested := range group {
		redacted := redacter.Redact(nested)
		if !redacted.Equal(slog.Attr{}) {
			safe = append(safe, redacted)
		}
	}
	return slog.Group(attribute.Key, attrsToAny(safe)...)
}

func (redacter Redacter) replacementAttr(key string) slog.Attr {
	replacement := redacter.replacement
	if replacement == "" {
		replacement = defaultReplacement
	}
	return slog.String(key, replacement)
}

func (redacter Redacter) sensitiveKey(key string) bool {
	if len(redacter.keys) == 0 {
		return false
	}
	normalized := strings.ToLower(strings.TrimSpace(key))
	if _, sensitive := redacter.keys[normalized]; sensitive {
		return true
	}
	segments := keySegments(key)
	for configured := range redacter.keys {
		if containsSegmentSequence(segments, compactKey(configured)) {
			return true
		}
	}
	return false
}

func keySegments(key string) []string {
	characters := []rune(strings.TrimSpace(key))
	segments := make([]string, 0, 4)
	start := -1
	flush := func(end int) {
		if start >= 0 {
			segments = append(
				segments,
				strings.ToLower(string(characters[start:end])),
			)
			start = -1
		}
	}
	for index, r := range characters {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			flush(index)
			continue
		}
		if start < 0 {
			start = index
			continue
		}
		previous := characters[index-1]
		camelBoundary := unicode.IsUpper(r) &&
			(unicode.IsLower(previous) || unicode.IsDigit(previous))
		acronymBoundary := unicode.IsUpper(r) &&
			unicode.IsUpper(previous) &&
			index+1 < len(characters) &&
			unicode.IsLower(characters[index+1])
		if camelBoundary || acronymBoundary {
			flush(index)
			start = index
		}
	}
	flush(len(characters))
	return segments
}

func compactKey(key string) string {
	var result strings.Builder
	for _, r := range key {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			result.WriteRune(unicode.ToLower(r))
		}
	}
	return result.String()
}

func containsSegmentSequence(segments []string, want string) bool {
	if len(segments) == 0 || want == "" {
		return false
	}
	for start := range segments {
		var candidate strings.Builder
		for _, segment := range segments[start:] {
			candidate.WriteString(segment)
			if candidate.Len() > len(want) {
				break
			}
			if candidate.String() == want {
				return true
			}
		}
	}
	return false
}

func attrsToAny(attributes []slog.Attr) []any {
	result := make([]any, len(attributes))
	for index, attribute := range attributes {
		result[index] = attribute
	}
	return result
}
