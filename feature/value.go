// Package feature provides provider-neutral, request-scoped feature
// evaluation over immutable revisioned definitions.
package feature

import (
	"errors"
	"fmt"
	"math"
	"unicode/utf8"
)

const maxStringValueBytes = 64 * 1024

var (
	// ErrInvalidDefinition reports a malformed complete flag definition.
	ErrInvalidDefinition = errors.New("feature: invalid definition")
	// ErrInvalidContext reports malformed targeting context.
	ErrInvalidContext = errors.New("feature: invalid evaluation context")
)

// Kind identifies the concrete type of a feature variation.
type Kind string

// Supported feature value kinds.
const (
	KindBoolean Kind = "boolean"
	KindString  Kind = "string"
	KindInteger Kind = "integer"
	KindFloat   Kind = "float"
)

// Value is one immutable, strongly typed feature variation value.
type Value struct {
	kind    Kind
	boolean bool
	text    string
	integer int64
	decimal float64
}

// BooleanValue constructs a Boolean Value.
func BooleanValue(value bool) Value {
	return Value{kind: KindBoolean, boolean: value}
}

// StringValue constructs a String Value.
func StringValue(value string) Value {
	return Value{kind: KindString, text: value}
}

// IntegerValue constructs an Integer Value.
func IntegerValue(value int64) Value {
	return Value{kind: KindInteger, integer: value}
}

// FloatValue constructs a finite Float Value.
func FloatValue(value float64) (Value, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return Value{}, fmt.Errorf("%w: float value must be finite", ErrInvalidDefinition)
	}
	return Value{kind: KindFloat, decimal: value}, nil
}

// Kind returns the concrete value kind.
func (value Value) Kind() Kind { return value.kind }

// Boolean returns the Boolean value when its kind matches.
func (value Value) Boolean() (bool, bool) {
	return value.boolean, value.kind == KindBoolean
}

// String returns the String value when its kind matches.
func (value Value) String() (string, bool) {
	return value.text, value.kind == KindString
}

// Integer returns the Integer value when its kind matches.
func (value Value) Integer() (int64, bool) {
	return value.integer, value.kind == KindInteger
}

// Float returns the Float value when its kind matches.
func (value Value) Float() (float64, bool) {
	return value.decimal, value.kind == KindFloat
}

func (value Value) valid() bool {
	switch value.kind {
	case KindBoolean, KindInteger:
		return true
	case KindString:
		return len(value.text) <= maxStringValueBytes && utf8.ValidString(value.text)
	case KindFloat:
		return !math.IsNaN(value.decimal) && !math.IsInf(value.decimal, 0)
	default:
		return false
	}
}
