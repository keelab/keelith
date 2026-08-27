package mysql

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	core "github.com/keelab/keelith/programmable/projection"
)

const (
	cursorPrefix          = "mysql/"
	cursorDigits          = 20
	maxChangePayloadBytes = 40 * 1024 * 1024
	maxMutationCount      = 10_000
	maxKeyBytes           = 4 * 1024
	maxValueBytes         = 16 * 1024 * 1024
	maxChangeidBytes      = 512
	mutationCodecVersion  = 1
)

func encodeCursor(offset uint64) core.Cursor {
	return core.Cursor(fmt.Sprintf("%s%0*d", cursorPrefix, cursorDigits, offset))
}

func decodeCursor(cursor core.Cursor) (uint64, error) {
	value := string(cursor)
	if len(value) != len(cursorPrefix)+cursorDigits || !strings.HasPrefix(value, cursorPrefix) {
		return 0, ErrInvalidCursor
	}
	offset := uint64(0)
	for _, character := range value[len(cursorPrefix):] {
		if character < '0' || character > '9' {
			return 0, ErrInvalidCursor
		}
		digit := uint64(character - '0')
		if offset > (math.MaxUint64-digit)/10 {
			return 0, ErrInvalidCursor
		}
		offset = offset*10 + digit
	}
	if offset > uint64(math.MaxInt64) || encodeCursor(offset) != cursor {
		return 0, ErrInvalidCursor
	}
	return offset, nil
}

func encodeMutations(mutations []core.Mutation) ([]core.Mutation, []byte, error) {
	if len(mutations) == 0 || len(mutations) > maxMutationCount {
		return nil, nil, fmt.Errorf("%w: mutation count", ErrInvalidOption)
	}
	cloned := cloneMutations(mutations)
	var buffer bytes.Buffer
	buffer.WriteByte(mutationCodecVersion)
	writeUint32(&buffer, uint32(len(cloned)))
	total := 0
	for index, mutation := range cloned {
		if err := mutation.Validate(); err != nil {
			return nil, nil, fmt.Errorf("%w: mutation %d: %w", ErrInvalidOption, index, err)
		}
		key, value := mutation.Key(), mutation.Value()
		if len(key) > maxKeyBytes || len(value) > maxValueBytes {
			return nil, nil, fmt.Errorf("%w: mutation %d key or value size", ErrInvalidOption, index)
		}
		total += len(key) + len(value)
		if total > 32*1024*1024 {
			return nil, nil, fmt.Errorf("%w: mutation bytes", ErrInvalidOption)
		}
		buffer.WriteByte(byte(mutation.Kind()))
		writeUint32(&buffer, uint32(len(key)))
		writeUint32(&buffer, uint32(len(value)))
		buffer.Write(key)
		buffer.Write(value)
		if buffer.Len() > maxChangePayloadBytes {
			return nil, nil, fmt.Errorf("%w: changelog payload", ErrInvalidOption)
		}
	}
	return cloned, buffer.Bytes(), nil
}

func decodeMutations(payload []byte) ([]core.Mutation, error) {
	if len(payload) < 5 || len(payload) > maxChangePayloadBytes {
		return nil, fmt.Errorf("%w: changelog payload size", ErrCorrupt)
	}
	reader := bytes.NewReader(payload)
	version, err := reader.ReadByte()
	if err != nil || version != mutationCodecVersion {
		return nil, fmt.Errorf("%w: changelog payload version", ErrCorrupt)
	}
	count, err := readUint32(reader)
	if err != nil || count == 0 || count > maxMutationCount {
		return nil, fmt.Errorf("%w: changelog mutation count", ErrCorrupt)
	}
	result := make([]core.Mutation, 0, count)
	total := 0
	for range count {
		kind, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("%w: mutation kind", ErrCorrupt)
		}
		keySize, err := readUint32(reader)
		if err != nil || keySize == 0 || keySize > maxKeyBytes {
			return nil, fmt.Errorf("%w: mutation key size", ErrCorrupt)
		}
		valueSize, err := readUint32(reader)
		if err != nil || valueSize > maxValueBytes {
			return nil, fmt.Errorf("%w: mutation value size", ErrCorrupt)
		}
		total += int(keySize) + int(valueSize)
		if total > 32*1024*1024 || uint64(keySize)+uint64(valueSize) > uint64(reader.Len()) {
			return nil, fmt.Errorf("%w: mutation payload size", ErrCorrupt)
		}
		key := make([]byte, keySize)
		value := make([]byte, valueSize)
		if _, err := io.ReadFull(reader, key); err != nil {
			return nil, fmt.Errorf("%w: mutation key", ErrCorrupt)
		}
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, fmt.Errorf("%w: mutation value", ErrCorrupt)
		}
		var mutation core.Mutation
		switch core.MutationKind(kind) {
		case core.MutationUpsert:
			mutation = core.Upsert(key, value)
		case core.MutationDelete:
			if len(value) != 0 {
				return nil, fmt.Errorf("%w: delete value", ErrCorrupt)
			}
			mutation = core.Delete(key)
		default:
			return nil, fmt.Errorf("%w: mutation kind", ErrCorrupt)
		}
		if err := mutation.Validate(); err != nil {
			return nil, fmt.Errorf("%w: invalid mutation", ErrCorrupt)
		}
		result = append(result, mutation)
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("%w: trailing changelog bytes", ErrCorrupt)
	}
	return result, nil
}

func writeUint32(writer io.Writer, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func readUint32(reader io.Reader) (uint32, error) {
	var encoded [4]byte
	if _, err := io.ReadFull(reader, encoded[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(encoded[:]), nil
}

func changeDigest(schema core.Schema, sourceTime time.Time, payload []byte) [sha256.Size]byte {
	digest := sha256.New()
	writeDigest(digest, []byte("keelith-mysql-projection-change-v1"))
	writeDigest(digest, []byte(schema.ID))
	writeDigest(digest, []byte(schema.Fingerprint))
	writeDigest(digest, []byte(schema.KeyFingerprint))
	var timestamp [8]byte
	binary.BigEndian.PutUint64(timestamp[:], uint64(sourceTime.UnixNano()))
	writeDigest(digest, timestamp[:])
	writeDigest(digest, payload)
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func writeDigest(digest hash.Hash, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = digest.Write(size[:])
	_, _ = digest.Write(value)
}

func cloneMutations(mutations []core.Mutation) []core.Mutation {
	result := make([]core.Mutation, len(mutations))
	for index, mutation := range mutations {
		result[index] = mutation.Clone()
	}
	return result
}

func validIdentity(value string, maximum int) bool {
	if value == "" || len(value) > maximum || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
