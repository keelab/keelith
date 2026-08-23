package cli

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

func identifier(value string) string {
	var result strings.Builder
	for _, r := range value {
		if unicode.IsLetter(r) ||
			unicode.IsDigit(r) ||
			r == '_' {
			result.WriteRune(r)
		} else {
			result.WriteByte('_')
		}
	}
	normalized := result.String()
	if normalized == "" {
		return "service"
	}
	first, _ := utf8.DecodeRuneInString(normalized)
	if !unicode.IsLetter(first) && first != '_' {
		return "service_" + normalized
	}
	return normalized
}
