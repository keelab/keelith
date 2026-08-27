package jwt

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"fmt"
	"math/big"
)

// StaticKey describes one explicitly identified asymmetric verification key.
type StaticKey struct {
	ID        string
	Algorithm string
	PublicKey any
}

type keyReference struct {
	keyid     string
	algorithm string
}

// StaticKeySet is an immutable in-process KeyProvider intended for controlled
// deployments, offline services, and tests.
type StaticKeySet struct {
	keys map[keyReference]any
}

// NewStaticKeySet validates and snapshots asymmetric public keys.
func NewStaticKeySet(values ...StaticKey) (*StaticKeySet, error) {
	if len(values) == 0 || len(values) > 128 {
		return nil, fmt.Errorf(
			"%w: static key count must be between 1 and 128",
			ErrInvalidConfig,
		)
	}
	keys := make(map[keyReference]any, len(values))
	for _, value := range values {
		if !validKeyid(value.ID) || !supportedAlgorithm(value.Algorithm) {
			return nil, fmt.Errorf(
				"%w: static key identity is malformed",
				ErrInvalidConfig,
			)
		}
		if err := validatePublicKey(value.Algorithm, value.PublicKey); err != nil {
			return nil, err
		}
		reference := keyReference{
			keyid:     value.ID,
			algorithm: value.Algorithm,
		}
		if _, duplicate := keys[reference]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate static key",
				ErrInvalidConfig,
			)
		}
		keys[reference] = clonePublicKey(value.PublicKey)
	}
	return &StaticKeySet{keys: keys}, nil
}

// Key resolves one exact key id and algorithm pair.
func (set *StaticKeySet) Key(
	ctx context.Context,
	keyid string,
	algorithm string,
) (any, error) {
	if ctx == nil {
		return nil, ErrKeyUnavailable
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if set == nil {
		return nil, ErrKeyUnavailable
	}
	key, found := set.keys[keyReference{
		keyid:     keyid,
		algorithm: algorithm,
	}]
	if !found {
		return nil, ErrKeyNotFound
	}
	return clonePublicKey(key), nil
}

func validKeyid(value string) bool {
	return validTokenValue(value, true) && len(value) <= 256
}

//nolint:staticcheck // EC key validation must inspect raw JWK-compatible coordinates.
func validatePublicKey(algorithm string, key any) error {
	switch algorithm {
	case "RS256", "RS384", "RS512", "PS256", "PS384", "PS512":
		publicKey, ok := key.(*rsa.PublicKey)
		if !ok || publicKey == nil ||
			publicKey.N == nil ||
			publicKey.N.BitLen() < 2048 ||
			publicKey.E < 3 ||
			publicKey.E%2 == 0 {
			return fmt.Errorf(
				"%w: invalid RSA public key",
				ErrInvalidConfig,
			)
		}
	case "ES256", "ES384", "ES512":
		publicKey, ok := key.(*ecdsa.PublicKey)
		if !ok || publicKey == nil ||
			publicKey.Curve == nil ||
			publicKey.X == nil ||
			publicKey.Y == nil ||
			!publicKey.Curve.IsOnCurve(publicKey.X, publicKey.Y) ||
			!matchingCurve(algorithm, publicKey.Curve) {
			return fmt.Errorf(
				"%w: invalid EC public key",
				ErrInvalidConfig,
			)
		}
	case "EdDSA":
		publicKey, ok := key.(ed25519.PublicKey)
		if !ok || len(publicKey) != ed25519.PublicKeySize {
			return fmt.Errorf(
				"%w: invalid Ed25519 public key",
				ErrInvalidConfig,
			)
		}
	default:
		return fmt.Errorf(
			"%w: unsupported asymmetric algorithm",
			ErrInvalidConfig,
		)
	}
	return nil
}

func matchingCurve(algorithm string, curve elliptic.Curve) bool {
	switch algorithm {
	case "ES256":
		return curve == elliptic.P256()
	case "ES384":
		return curve == elliptic.P384()
	case "ES512":
		return curve == elliptic.P521()
	default:
		return false
	}
}

//nolint:staticcheck // EC key cloning preserves raw coordinates for caller-owned keys.
func clonePublicKey(key any) any {
	switch typed := key.(type) {
	case *rsa.PublicKey:
		if typed == nil {
			return (*rsa.PublicKey)(nil)
		}
		return &rsa.PublicKey{
			N: new(big.Int).Set(typed.N),
			E: typed.E,
		}
	case *ecdsa.PublicKey:
		if typed == nil {
			return (*ecdsa.PublicKey)(nil)
		}
		return &ecdsa.PublicKey{
			Curve: typed.Curve,
			X:     new(big.Int).Set(typed.X),
			Y:     new(big.Int).Set(typed.Y),
		}
	case ed25519.PublicKey:
		return append(ed25519.PublicKey(nil), typed...)
	default:
		return nil
	}
}
