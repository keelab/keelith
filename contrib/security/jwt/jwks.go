package jwt

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
)

type jwksDocument struct {
	Keys []jwkDocument `json:"keys"`
}

type jwkDocument struct {
	KeyType   string   `json:"kty"`
	Use       string   `json:"use"`
	KeyOps    []string `json:"key_ops"`
	Algorithm string   `json:"alg"`
	Keyid     string   `json:"kid"`
	Modulus   string   `json:"n"`
	Exponent  string   `json:"e"`
	Curve     string   `json:"crv"`
	X         string   `json:"x"`
	Y         string   `json:"y"`
}

func parseJWKS(
	body []byte,
	allowed map[string]struct{},
) (map[keyReference]any, error) {
	var document jwksDocument
	if err := json.Unmarshal(body, &document); err != nil ||
		len(document.Keys) == 0 ||
		len(document.Keys) > maxJWKSKeys {
		return nil, ErrKeyUnavailable
	}
	keys := make(map[keyReference]any, len(document.Keys))
	for _, keyDocument := range document.Keys {
		if keyDocument.Use != "" && keyDocument.Use != "sig" {
			continue
		}
		if len(keyDocument.KeyOps) > 0 &&
			!contains(keyDocument.KeyOps, "verify") {
			continue
		}
		if _, accepted := allowed[keyDocument.Algorithm]; !accepted {
			continue
		}
		if !validKeyid(keyDocument.Keyid) {
			return nil, ErrKeyUnavailable
		}
		publicKey, err := parseJWK(keyDocument)
		if err != nil {
			return nil, ErrKeyUnavailable
		}
		if err := validatePublicKey(
			keyDocument.Algorithm,
			publicKey,
		); err != nil {
			return nil, ErrKeyUnavailable
		}
		reference := keyReference{
			keyid:     keyDocument.Keyid,
			algorithm: keyDocument.Algorithm,
		}
		if _, duplicate := keys[reference]; duplicate {
			return nil, ErrKeyUnavailable
		}
		keys[reference] = publicKey
	}
	if len(keys) == 0 {
		return nil, ErrKeyUnavailable
	}
	return keys, nil
}

//nolint:staticcheck // JWK EC keys are encoded as raw coordinates by RFC 7517.
func parseJWK(document jwkDocument) (any, error) {
	switch document.KeyType {
	case "RSA":
		if document.Algorithm != "RS256" &&
			document.Algorithm != "RS384" &&
			document.Algorithm != "RS512" &&
			document.Algorithm != "PS256" &&
			document.Algorithm != "PS384" &&
			document.Algorithm != "PS512" {
			return nil, ErrKeyUnavailable
		}
		modulus, err := decodeInteger(document.Modulus, 1024)
		if err != nil {
			return nil, err
		}
		exponentBytes, err := decodeBase64(document.Exponent, 8)
		if err != nil || len(exponentBytes) == 0 {
			return nil, ErrKeyUnavailable
		}
		exponent := 0
		for _, value := range exponentBytes {
			if exponent > (int(^uint(0)>>1)-int(value))/256 {
				return nil, ErrKeyUnavailable
			}
			exponent = exponent*256 + int(value)
		}
		return &rsa.PublicKey{N: modulus, E: exponent}, nil
	case "EC":
		curve, expectedAlgorithm := jwkCurve(document.Curve)
		if curve == nil || expectedAlgorithm != document.Algorithm {
			return nil, ErrKeyUnavailable
		}
		x, err := decodeInteger(document.X, 128)
		if err != nil {
			return nil, err
		}
		y, err := decodeInteger(document.Y, 128)
		if err != nil {
			return nil, err
		}
		return &ecdsa.PublicKey{Curve: curve, X: x, Y: y}, nil
	case "OKP":
		if document.Curve != "Ed25519" || document.Algorithm != "EdDSA" {
			return nil, ErrKeyUnavailable
		}
		x, err := decodeBase64(document.X, ed25519.PublicKeySize)
		if err != nil || len(x) != ed25519.PublicKeySize {
			return nil, ErrKeyUnavailable
		}
		return ed25519.PublicKey(x), nil
	default:
		return nil, ErrKeyUnavailable
	}
}

func decodeInteger(value string, maximum int) (*big.Int, error) {
	decoded, err := decodeBase64(value, maximum)
	if err != nil || len(decoded) == 0 {
		return nil, ErrKeyUnavailable
	}
	integer := new(big.Int).SetBytes(decoded)
	if integer.Sign() <= 0 {
		return nil, ErrKeyUnavailable
	}
	return integer, nil
}

func decodeBase64(value string, maximum int) ([]byte, error) {
	if value == "" || len(value) > maximum*2 {
		return nil, ErrKeyUnavailable
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(decoded) > maximum {
		return nil, fmt.Errorf("%w", ErrKeyUnavailable)
	}
	return decoded, nil
}

func jwkCurve(name string) (elliptic.Curve, string) {
	switch name {
	case "P-256":
		return elliptic.P256(), "ES256"
	case "P-384":
		return elliptic.P384(), "ES384"
	case "P-521":
		return elliptic.P521(), "ES512"
	default:
		return nil, ""
	}
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
