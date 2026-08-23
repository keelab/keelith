// Package httptemplate parses and executes google.api.http path templates.
package httptemplate

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	maximumTemplateBytes     = 4096
	maximumTemplateSegments  = 64
	maximumTemplateVariables = 16
)

// Wildcard identifies one variable pattern segment.
type Wildcard uint8

const (
	// NoWildcard identifies a literal pattern segment.
	NoWildcard Wildcard = iota
	// SingleWildcard identifies a "*" path segment.
	SingleWildcard
	// MultiWildcard identifies a terminal "**" path segment.
	MultiWildcard
)

// PatternSegment is one literal or wildcard inside a variable assignment.
type PatternSegment struct {
	Literal  string
	Wildcard Wildcard
}

// Variable is one FieldPath and its normalized segment pattern.
type Variable struct {
	FieldPath string
	Pattern   []PatternSegment
	Explicit  bool
}

// CaptureCount reports the number of router captures needed by the variable.
func (variable Variable) CaptureCount() int {
	count := 0
	for _, segment := range variable.Pattern {
		if segment.Wildcard != NoWildcard {
			count++
		}
	}
	return count
}

// RequiresString reports whether the complete matched resource name, rather
// than one scalar segment, is assigned to the field.
func (variable Variable) RequiresString() bool {
	return variable.Explicit && (len(variable.Pattern) != 1 ||
		variable.Pattern[0].Wildcard != SingleWildcard)
}

// PatternString returns the normalized assignment pattern.
func (variable Variable) PatternString() string {
	parts := make([]string, 0, len(variable.Pattern))
	for _, segment := range variable.Pattern {
		switch segment.Wildcard {
		case SingleWildcard:
			parts = append(parts, "*")
		case MultiWildcard:
			parts = append(parts, "**")
		default:
			parts = append(parts, segment.Literal)
		}
	}
	return strings.Join(parts, "/")
}

// HasMultiWildcard reports whether the pattern has a terminal "**".
func (variable Variable) HasMultiWildcard() bool {
	return len(variable.Pattern) > 0 &&
		variable.Pattern[len(variable.Pattern)-1].Wildcard == MultiWildcard
}

// Segment is one top-level literal or variable segment.
type Segment struct {
	Literal  string
	Variable *Variable
}

// Template is a validated google.api.http path template.
type Template struct {
	Segments []Segment
	Verb     string
}

// Parse validates a google.api.http path template and returns its AST.
// Anonymous top-level wildcards are rejected because generated clients cannot
// derive their values from a request message.
func Parse(raw string) (*Template, error) {
	if raw == "" || len(raw) > maximumTemplateBytes || raw[0] != '/' ||
		strings.ContainsAny(raw, "?#\r\n\t") {
		return nil, fmt.Errorf("http template: invalid path %q", raw)
	}
	path, verb, err := splitVerb(raw)
	if err != nil {
		return nil, err
	}
	rawSegments, err := splitTopLevelSegments(path)
	if err != nil {
		return nil, err
	}
	if len(rawSegments) == 0 || len(rawSegments) > maximumTemplateSegments {
		return nil, fmt.Errorf("http template: invalid segment count")
	}
	result := &Template{
		Segments: make([]Segment, 0, len(rawSegments)),
		Verb:     verb,
	}
	seen := make(map[string]struct{})
	variableCount := 0
	for index, rawSegment := range rawSegments {
		if rawSegment == "" {
			return nil, fmt.Errorf("http template: empty path segment")
		}
		if rawSegment == "*" || rawSegment == "**" {
			return nil, fmt.Errorf(
				"http template: anonymous wildcard %q is not reversible",
				rawSegment,
			)
		}
		if strings.HasPrefix(rawSegment, "{") &&
			strings.HasSuffix(rawSegment, "}") {
			variable, parseErr := parseVariable(
				rawSegment[1 : len(rawSegment)-1],
			)
			if parseErr != nil {
				return nil, parseErr
			}
			if _, duplicate := seen[variable.FieldPath]; duplicate {
				return nil, fmt.Errorf(
					"http template: duplicate field path %q",
					variable.FieldPath,
				)
			}
			seen[variable.FieldPath] = struct{}{}
			variableCount++
			if variableCount > maximumTemplateVariables {
				return nil, fmt.Errorf("http template: too many variables")
			}
			if variable.HasMultiWildcard() && index != len(rawSegments)-1 {
				return nil, fmt.Errorf(
					"http template: ** must terminate the path",
				)
			}
			result.Segments = append(result.Segments, Segment{
				Variable: &variable,
			})
			continue
		}
		if strings.ContainsAny(rawSegment, "{}") {
			return nil, fmt.Errorf(
				"http template: malformed variable segment %q",
				rawSegment,
			)
		}
		if err := validateLiteral(rawSegment); err != nil {
			return nil, err
		}
		result.Segments = append(result.Segments, Segment{
			Literal: rawSegment,
		})
	}
	return result, nil
}

// Variables returns variables in path order.
func (template *Template) Variables() []Variable {
	if template == nil {
		return nil
	}
	result := make([]Variable, 0)
	for _, segment := range template.Segments {
		if segment.Variable == nil {
			continue
		}
		variable := *segment.Variable
		variable.Pattern = append([]PatternSegment(nil), variable.Pattern...)
		result = append(result, variable)
	}
	return result
}

// EndsWithVariable reports whether a custom verb would be attached directly
// to a generated router wildcard.
func (template *Template) EndsWithVariable() bool {
	return template != nil && len(template.Segments) > 0 &&
		template.Segments[len(template.Segments)-1].Variable != nil
}

// Render converts variables to router-specific capture segments. The callback
// returns a complete router segment such as "{name}", "{name...}", ":name",
// or "*name".
func (template *Template) Render(
	capture func(variableIndex int, captureIndex int, multi bool) string,
) (string, error) {
	if template == nil || capture == nil {
		return "", fmt.Errorf("http template: render input is nil")
	}
	parts := make([]string, 0, len(template.Segments))
	variableIndex := 0
	for _, segment := range template.Segments {
		if segment.Variable == nil {
			parts = append(parts, segment.Literal)
			continue
		}
		captureIndex := 0
		for _, pattern := range segment.Variable.Pattern {
			if pattern.Wildcard == NoWildcard {
				parts = append(parts, pattern.Literal)
				continue
			}
			rendered := capture(
				variableIndex,
				captureIndex,
				pattern.Wildcard == MultiWildcard,
			)
			if rendered == "" || strings.Contains(rendered, "/") {
				return "", fmt.Errorf("http template: invalid rendered capture")
			}
			parts = append(parts, rendered)
			captureIndex++
		}
		variableIndex++
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("http template: rendered path is empty")
	}
	if template.Verb != "" {
		parts[len(parts)-1] += ":" + template.Verb
	}
	return "/" + strings.Join(parts, "/"), nil
}

// Expand validates field values against variable patterns and creates an
// escaped request path.
func (template *Template) Expand(values map[string]string) (string, error) {
	if template == nil {
		return "", fmt.Errorf("http template: template is nil")
	}
	parts := make([]string, 0, len(template.Segments))
	for _, segment := range template.Segments {
		if segment.Variable == nil {
			parts = append(parts, segment.Literal)
			continue
		}
		value, exists := values[segment.Variable.FieldPath]
		if !exists {
			return "", fmt.Errorf(
				"http template: field %q is absent",
				segment.Variable.FieldPath,
			)
		}
		expanded, err := expandVariable(*segment.Variable, value)
		if err != nil {
			return "", err
		}
		parts = append(parts, expanded...)
	}
	if len(parts) == 0 {
		return "", fmt.Errorf("http template: expanded path is empty")
	}
	if template.Verb != "" {
		parts[len(parts)-1] += ":" + template.Verb
	}
	return "/" + strings.Join(parts, "/"), nil
}

// Match extracts complete field values from an escaped request path.
func (template *Template) Match(escapedPath string) (map[string]string, error) {
	if template == nil || escapedPath == "" || escapedPath[0] != '/' ||
		strings.ContainsAny(escapedPath, "?#\r\n") {
		return nil, fmt.Errorf("http template: invalid request path")
	}
	rawParts := strings.Split(strings.TrimPrefix(escapedPath, "/"), "/")
	if template.Verb != "" {
		if len(rawParts) == 0 {
			return nil, fmt.Errorf("http template: request verb is absent")
		}
		last := rawParts[len(rawParts)-1]
		colon := strings.LastIndexByte(last, ':')
		if colon < 0 {
			return nil, fmt.Errorf("http template: request verb is absent")
		}
		gotVerb, err := decodePathSegment(last[colon+1:], false)
		if err != nil {
			return nil, err
		}
		wantVerb, _ := decodePathSegment(template.Verb, false)
		if gotVerb != wantVerb {
			return nil, fmt.Errorf("http template: request verb does not match")
		}
		rawParts[len(rawParts)-1] = last[:colon]
	}
	values := make(map[string]string)
	pathIndex := 0
	for _, segment := range template.Segments {
		if segment.Variable == nil {
			if pathIndex >= len(rawParts) {
				return nil, fmt.Errorf("http template: request path is too short")
			}
			if err := matchLiteral(segment.Literal, rawParts[pathIndex]); err != nil {
				return nil, err
			}
			pathIndex++
			continue
		}
		value, consumed, err := matchVariable(
			*segment.Variable,
			rawParts[pathIndex:],
		)
		if err != nil {
			return nil, err
		}
		values[segment.Variable.FieldPath] = value
		pathIndex += consumed
	}
	if pathIndex != len(rawParts) {
		return nil, fmt.Errorf("http template: request path is too long")
	}
	return values, nil
}

func parseVariable(raw string) (Variable, error) {
	parts := strings.SplitN(raw, "=", 2)
	fieldPath := parts[0]
	if !validFieldPath(fieldPath) {
		return Variable{}, fmt.Errorf(
			"http template: invalid field path %q",
			fieldPath,
		)
	}
	pattern := "*"
	explicit := false
	if len(parts) == 2 {
		pattern = parts[1]
		explicit = true
	}
	if pattern == "" || strings.ContainsAny(pattern, "{}") {
		return Variable{}, fmt.Errorf(
			"http template: invalid pattern for %q",
			fieldPath,
		)
	}
	rawSegments := strings.Split(pattern, "/")
	if len(rawSegments) > maximumTemplateSegments {
		return Variable{}, fmt.Errorf("http template: variable pattern is too long")
	}
	segments := make([]PatternSegment, 0, len(rawSegments))
	for index, rawSegment := range rawSegments {
		switch rawSegment {
		case "*":
			segments = append(segments, PatternSegment{Wildcard: SingleWildcard})
		case "**":
			if index != len(rawSegments)-1 {
				return Variable{}, fmt.Errorf(
					"http template: ** must terminate a variable pattern",
				)
			}
			segments = append(segments, PatternSegment{Wildcard: MultiWildcard})
		default:
			if rawSegment == "" {
				return Variable{}, fmt.Errorf(
					"http template: empty variable pattern segment",
				)
			}
			if err := validateLiteral(rawSegment); err != nil {
				return Variable{}, err
			}
			segments = append(segments, PatternSegment{Literal: rawSegment})
		}
	}
	return Variable{
		FieldPath: fieldPath,
		Pattern:   segments,
		Explicit:  explicit,
	}, nil
}

func splitVerb(raw string) (string, string, error) {
	depth := 0
	colon := -1
	for index, r := range raw {
		switch r {
		case '{':
			if depth != 0 {
				return "", "", fmt.Errorf("http template: nested variable")
			}
			depth++
		case '}':
			depth--
			if depth < 0 {
				return "", "", fmt.Errorf("http template: unmatched }")
			}
		case ':':
			if depth == 0 {
				if colon >= 0 {
					return "", "", fmt.Errorf("http template: multiple verbs")
				}
				colon = index
			}
		case '/':
			if depth == 0 && colon >= 0 {
				return "", "", fmt.Errorf("http template: verb must be terminal")
			}
		}
	}
	if depth != 0 {
		return "", "", fmt.Errorf("http template: unmatched {")
	}
	if colon < 0 {
		return raw, "", nil
	}
	verb := raw[colon+1:]
	if verb == "" {
		return "", "", fmt.Errorf("http template: empty verb")
	}
	if err := validateLiteral(verb); err != nil {
		return "", "", err
	}
	return raw[:colon], verb, nil
}

func splitTopLevelSegments(path string) ([]string, error) {
	if path == "/" {
		return nil, nil
	}
	result := make([]string, 0)
	depth := 0
	start := 1
	for index := 1; index < len(path); index++ {
		switch path[index] {
		case '{':
			if depth != 0 {
				return nil, fmt.Errorf("http template: nested variable")
			}
			depth++
		case '}':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("http template: unmatched }")
			}
		case '/':
			if depth == 0 {
				result = append(result, path[start:index])
				start = index + 1
			}
		}
	}
	if depth != 0 {
		return nil, fmt.Errorf("http template: unmatched {")
	}
	result = append(result, path[start:])
	return result, nil
}

func validateLiteral(raw string) error {
	if raw == "" || strings.ContainsAny(raw, "{}*/:?#\r\n\t ") {
		return fmt.Errorf("http template: invalid literal %q", raw)
	}
	if _, err := decodePathSegment(raw, false); err != nil {
		return fmt.Errorf("http template: invalid literal %q: %w", raw, err)
	}
	return nil
}

func validFieldPath(value string) bool {
	if value == "" || len(value) > 1024 || strings.TrimSpace(value) != value {
		return false
	}
	segments := strings.Split(value, ".")
	if len(segments) > 16 {
		return false
	}
	for _, segment := range segments {
		if segment == "" {
			return false
		}
		for index, r := range segment {
			valid := r == '_' ||
				r >= 'a' && r <= 'z' ||
				r >= 'A' && r <= 'Z' ||
				index > 0 && r >= '0' && r <= '9'
			if !valid {
				return false
			}
		}
	}
	return true
}

func expandVariable(variable Variable, value string) ([]string, error) {
	if len(variable.Pattern) == 1 &&
		variable.Pattern[0].Wildcard == SingleWildcard {
		if value == "" {
			return nil, variableMismatch(variable, value)
		}
		return []string{encodePathSegment(value)}, nil
	}
	valueParts := strings.Split(value, "/")
	result := make([]string, 0, len(variable.Pattern))
	valueIndex := 0
	for _, pattern := range variable.Pattern {
		switch pattern.Wildcard {
		case NoWildcard:
			if valueIndex >= len(valueParts) {
				return nil, variableMismatch(variable, value)
			}
			literal, _ := decodePathSegment(pattern.Literal, false)
			if valueParts[valueIndex] != literal {
				return nil, variableMismatch(variable, value)
			}
			result = append(result, pattern.Literal)
			valueIndex++
		case SingleWildcard:
			if valueIndex >= len(valueParts) || valueParts[valueIndex] == "" {
				return nil, variableMismatch(variable, value)
			}
			result = append(result, encodePathSegment(valueParts[valueIndex]))
			valueIndex++
		case MultiWildcard:
			for ; valueIndex < len(valueParts); valueIndex++ {
				if valueParts[valueIndex] == "" {
					return nil, variableMismatch(variable, value)
				}
				result = append(result, encodePathSegment(valueParts[valueIndex]))
			}
		}
	}
	if valueIndex != len(valueParts) {
		return nil, variableMismatch(variable, value)
	}
	return result, nil
}

func matchVariable(
	variable Variable,
	rawParts []string,
) (string, int, error) {
	if len(variable.Pattern) == 1 &&
		variable.Pattern[0].Wildcard == SingleWildcard {
		if len(rawParts) == 0 || rawParts[0] == "" {
			return "", 0, variableMismatch(variable, "")
		}
		value, err := decodePathSegment(rawParts[0], false)
		return value, 1, err
	}
	valueParts := make([]string, 0, len(variable.Pattern))
	pathIndex := 0
	for _, pattern := range variable.Pattern {
		switch pattern.Wildcard {
		case NoWildcard:
			if pathIndex >= len(rawParts) {
				return "", 0, variableMismatch(variable, "")
			}
			if err := matchLiteral(pattern.Literal, rawParts[pathIndex]); err != nil {
				return "", 0, err
			}
			literal, _ := decodePathSegment(pattern.Literal, false)
			valueParts = append(valueParts, literal)
			pathIndex++
		case SingleWildcard:
			if pathIndex >= len(rawParts) || rawParts[pathIndex] == "" {
				return "", 0, variableMismatch(variable, "")
			}
			value, err := decodePathSegment(rawParts[pathIndex], false)
			if err != nil {
				return "", 0, err
			}
			valueParts = append(valueParts, value)
			pathIndex++
		case MultiWildcard:
			value, err := decodeMultiPath(rawParts[pathIndex:])
			if err != nil {
				return "", 0, err
			}
			if value != "" {
				valueParts = append(valueParts, value)
			}
			pathIndex = len(rawParts)
		}
	}
	return strings.Join(valueParts, "/"), pathIndex, nil
}

func matchLiteral(expected string, raw string) error {
	want, _ := decodePathSegment(expected, false)
	got, err := decodePathSegment(raw, false)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf(
			"http template: literal %q does not match %q",
			got,
			want,
		)
	}
	return nil
}

func variableMismatch(variable Variable, value string) error {
	return fmt.Errorf(
		"http template: field %q value %q does not match %q",
		variable.FieldPath,
		value,
		variable.PatternString(),
	)
}

func encodePathSegment(value string) string {
	const hexadecimal = "0123456789ABCDEF"
	var builder strings.Builder
	for index := 0; index < len(value); index++ {
		r := value[index]
		unreserved := r >= 'a' && r <= 'z' ||
			r >= 'A' && r <= 'Z' ||
			r >= '0' && r <= '9' ||
			r == '-' || r == '_' || r == '.' ||
			r == '~'
		if unreserved {
			builder.WriteByte(r)
			continue
		}
		builder.WriteByte('%')
		builder.WriteByte(hexadecimal[r>>4])
		builder.WriteByte(hexadecimal[r&0x0f])
	}
	return builder.String()
}

func decodeMultiPath(parts []string) (string, error) {
	if len(parts) == 1 && parts[0] == "" {
		return "", nil
	}
	decoded := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			return "", fmt.Errorf("http template: empty multi path segment")
		}
		value, err := decodePathSegment(part, true)
		if err != nil {
			return "", err
		}
		decoded = append(decoded, value)
	}
	return strings.Join(decoded, "/"), nil
}

func decodePathSegment(raw string, preserveSlash bool) (string, error) {
	var builder strings.Builder
	for index := 0; index < len(raw); index++ {
		if raw[index] != '%' {
			builder.WriteByte(raw[index])
			continue
		}
		if index+2 >= len(raw) {
			return "", fmt.Errorf("http template: invalid percent escape")
		}
		high, highOK := fromHex(raw[index+1])
		low, lowOK := fromHex(raw[index+2])
		if !highOK || !lowOK {
			return "", fmt.Errorf("http template: invalid percent escape")
		}
		value := high<<4 | low
		if preserveSlash && value == '/' {
			builder.WriteString(raw[index : index+3])
		} else {
			builder.WriteByte(value)
		}
		index += 2
	}
	result := builder.String()
	if !utf8.ValidString(result) {
		return "", fmt.Errorf("http template: path is not valid UTF-8")
	}
	return result, nil
}

func fromHex(r byte) (byte, bool) {
	switch {
	case r >= '0' && r <= '9':
		return r - '0', true
	case r >= 'a' && r <= 'f':
		return r - 'a' + 10, true
	case r >= 'A' && r <= 'F':
		return r - 'A' + 10, true
	default:
		return 0, false
	}
}
