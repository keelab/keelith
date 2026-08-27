package cache

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

var associatedData = []byte("keelith/config-cache/v1")

// Cipher encrypts and authenticates cached snapshot payloads.
type Cipher interface {
	Seal([]byte) (nonce []byte, ciphertext []byte, err error)
	Open(nonce []byte, ciphertext []byte) ([]byte, error)
}

// AESGCM is an authenticated AES-GCM snapshot cipher.
type AESGCM struct {
	aead cipher.AEAD
}

// NewAESGCM constructs AES-GCM using a 16, 24, or 32-byte key.
func NewAESGCM(key []byte) (*AESGCM, error) {
	block, err := aes.NewCipher(append([]byte(nil), key...))
	if err != nil {
		return nil, fmt.Errorf("%w: AES key: %w", ErrInvalidOption, err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: AES-GCM: %w", ErrInvalidOption, err)
	}
	return &AESGCM{aead: aead}, nil
}

// Seal encrypts and authenticates plaintext with a fresh random nonce.
func (cipher *AESGCM) Seal(
	plaintext []byte,
) ([]byte, []byte, error) {
	if cipher == nil || cipher.aead == nil {
		return nil, nil, fmt.Errorf("%w: cipher is nil", ErrInvalidOption)
	}
	nonce := make([]byte, cipher.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, fmt.Errorf("config cache: nonce: %w", err)
	}
	ciphertext := cipher.aead.Seal(
		nil,
		nonce,
		plaintext,
		associatedData,
	)
	return nonce, ciphertext, nil
}

// Open authenticates and decrypts ciphertext.
func (cipher *AESGCM) Open(
	nonce []byte,
	ciphertext []byte,
) ([]byte, error) {
	if cipher == nil || cipher.aead == nil {
		return nil, fmt.Errorf("%w: cipher is nil", ErrInvalidOption)
	}
	if len(nonce) != cipher.aead.NonceSize() {
		return nil, fmt.Errorf("%w: invalid nonce", ErrCorrupt)
	}
	plaintext, err := cipher.aead.Open(
		nil,
		nonce,
		ciphertext,
		associatedData,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: authentication failed", ErrCorrupt)
	}
	return plaintext, nil
}
