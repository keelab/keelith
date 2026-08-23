package cli

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

func identifier(value string) string {
	var result strings.Builder
	for _, character := range value {
		if unicode.IsLetter(character) ||
			unicode.IsDigit(character) ||
			character == '_' {
			result.WriteRune(character)
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
