package generator

import (
	"fmt"
	"math"
	"strings"
	"unicode"

	"google.golang.org/protobuf/compiler/protogen"
)

const (
	minGeneratedErrorCode = 400
	maxGeneratedErrorCode = 599
)

type errorDefinition struct {
	Code   int32
	Reason string
	Symbol string
}

func collectErrorDefinitions(file *protogen.File) ([]errorDefinition, error) {
	result := make([]errorDefinition, 0)
	symbols := make(map[string]string)
	for _, enum := range file.Enums {
		if err := collectEnumErrorDefinitions(enum, symbols, &result); err != nil {
			return nil, err
		}
	}
	for _, message := range file.Messages {
		if err := collectMessageErrorDefinitions(
			message,
			symbols,
			&result,
		); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func collectMessageErrorDefinitions(
	message *protogen.Message,
	symbols map[string]string,
	target *[]errorDefinition,
) error {
	for _, enum := range message.Enums {
		if err := collectEnumErrorDefinitions(enum, symbols, target); err != nil {
			return err
		}
	}
	for _, nested := range message.Messages {
		if err := collectMessageErrorDefinitions(nested, symbols, target); err != nil {
			return err
		}
	}
	return nil
}

func collectEnumErrorDefinitions(
	enum *protogen.Enum,
	symbols map[string]string,
	target *[]errorDefinition,
) error {
	for _, value := range enum.Values {
		code, ok, err := enumValueErrorCode(value)
		if err != nil {
			return fmt.Errorf("%s: %w", value.Desc.FullName(), err)
		}
		if !ok {
			continue
		}
		if value.Desc.Number() == 0 {
			return fmt.Errorf(
				"%s: zero enum value cannot declare error_code",
				value.Desc.FullName(),
			)
		}
		symbol, err := errorDefinitionSymbol(enum, value)
		if err != nil {
			return fmt.Errorf("%s: %w", value.Desc.FullName(), err)
		}
		reason := string(value.Desc.Name())
		if previous, duplicate := symbols[symbol]; duplicate {
			return fmt.Errorf(
				"generated error symbol %s conflicts between %s and %s",
				symbol,
				previous,
				value.Desc.FullName(),
			)
		}
		symbols[symbol] = string(value.Desc.FullName())
		*target = append(*target, errorDefinition{
			Code:   code,
			Reason: reason,
			Symbol: symbol,
		})
	}
	return nil
}

func enumValueErrorCode(value *protogen.EnumValue) (int32, bool, error) {
	raw, ok, err := unknownVarint(
		value.Desc.Options().ProtoReflect().GetUnknown(),
		errorCodeFieldNumber,
	)
	if err != nil || !ok {
		return 0, false, err
	}
	if raw > math.MaxInt32 {
		return 0, false, fmt.Errorf("error_code overflows int32")
	}
	code := int32(raw)
	if code < minGeneratedErrorCode || code > maxGeneratedErrorCode {
		return 0, false, fmt.Errorf(
			"error_code %d must be between %d and %d",
			code,
			minGeneratedErrorCode,
			maxGeneratedErrorCode,
		)
	}
	return code, true, nil
}

func errorDefinitionSymbol(
	enum *protogen.Enum,
	value *protogen.EnumValue,
) (string, error) {
	enumName := strings.ReplaceAll(enum.GoIdent.GoName, "_", "")
	switch {
	case strings.HasSuffix(enumName, "ErrorReason"):
		enumName = strings.TrimSuffix(enumName, "ErrorReason")
	case strings.HasSuffix(enumName, "Error"):
		enumName = strings.TrimSuffix(enumName, "Error")
	}
	if enumName == "" {
		enumName = "Framework"
	}
	valueName := string(value.Desc.Name())
	prefix := screamingSnake(string(enum.Desc.Name()))
	suffix := valueName
	if strings.HasPrefix(valueName, prefix+"_") {
		suffix = strings.TrimPrefix(valueName, prefix+"_")
	}
	suffix = protoIdentifierToGo(suffix)
	if suffix == "" {
		return "", fmt.Errorf("error enum or value name has no generated symbol")
	}
	return enumName + suffix, nil
}

func screamingSnake(value string) string {
	var output strings.Builder
	runes := []rune(value)
	for index, character := range runes {
		if unicode.IsUpper(character) && index > 0 {
			previous := runes[index-1]
			var next rune
			if index+1 < len(runes) {
				next = runes[index+1]
			}
			if unicode.IsLower(previous) ||
				unicode.IsDigit(previous) ||
				unicode.IsUpper(previous) && unicode.IsLower(next) {
				output.WriteByte('_')
			}
		}
		output.WriteRune(unicode.ToUpper(character))
	}
	return output.String()
}

func protoIdentifierToGo(value string) string {
	var output strings.Builder
	upperNext := true
	for _, character := range value {
		if character == '_' {
			upperNext = true
			continue
		}
		if upperNext {
			output.WriteRune(unicode.ToUpper(character))
			upperNext = false
			continue
		}
		output.WriteRune(unicode.ToLower(character))
	}
	return output.String()
}

func generateErrorDefinitions(
	output *protogen.GeneratedFile,
	definitions []errorDefinition,
) {
	for _, definition := range definitions {
		reasonName := definition.Symbol + "Reason"
		codeName := definition.Symbol + "Code"
		errorName := "Err" + definition.Symbol
		constructorName := "New" + definition.Symbol
		wrapperName := "Wrap" + definition.Symbol

		output.P(
			"// ",
			reasonName,
			" is the stable machine reason declared by the Proto enum.",
		)
		output.P("const ", reasonName, " = ", fmt.Sprintf("%q", definition.Reason))
		output.P(
			"// ",
			codeName,
			" is the stable transport-neutral application code.",
		)
		output.P("const ", codeName, " int32 = ", definition.Code)
		output.P(
			"// ",
			errorName,
			" is the immutable identity sentinel for this error.",
		)
		output.P(
			"var ",
			errorName,
			" = ",
			output.QualifiedGoIdent(errorsPackage.Ident("New")),
			"(",
			codeName,
			", ",
			reasonName,
			", \"\")",
		)
		output.P(
			"// ",
			constructorName,
			" constructs this declared application error.",
		)
		output.P(
			"func ",
			constructorName,
			"(message string, options ...",
			output.QualifiedGoIdent(errorsPackage.Ident("Option")),
			") *",
			output.QualifiedGoIdent(errorsPackage.Ident("Error")),
			" {",
		)
		output.P(
			"return ",
			output.QualifiedGoIdent(errorsPackage.Ident("New")),
			"(",
			codeName,
			", ",
			reasonName,
			", message, options...)",
		)
		output.P("}")
		output.P(
			"// ",
			wrapperName,
			" preserves a private cause while exposing this declared identity.",
		)
		output.P(
			"func ",
			wrapperName,
			"(cause error, message string, options ...",
			output.QualifiedGoIdent(errorsPackage.Ident("Option")),
			") *",
			output.QualifiedGoIdent(errorsPackage.Ident("Error")),
			" {",
		)
		output.P(
			"return ",
			output.QualifiedGoIdent(errorsPackage.Ident("Wrap")),
			"(cause, ",
			codeName,
			", ",
			reasonName,
			", message, options...)",
		)
		output.P("}")
		output.P()
	}
}
