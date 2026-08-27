// Package mysql provides an InnoDB-backed projection owner Source.
package mysql

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const defaultTablePrefix = "keelith_projection"

// LatestMigrationVersion is the newest MySQL projection storage migration.
const LatestMigrationVersion = 1

var (
	// ErrInvalidOption reports an invalid database, schema, table, or budget.
	ErrInvalidOption = errors.New("mysql projection: invalid option")
	// ErrNotSeeded reports subscription before the first committed change.
	ErrNotSeeded = errors.New("mysql projection: owner is not seeded")
	// ErrSourceClosed reports use after Source.Close.
	ErrSourceClosed = errors.New("mysql projection: source closed")
	// ErrSessionClosed reports Next after Session.Close.
	ErrSessionClosed = errors.New("mysql projection: session closed")
	// ErrInvalidCursor reports a malformed or future MySQL cursor.
	ErrInvalidCursor = errors.New("mysql projection: invalid cursor")
	// ErrCorrupt reports inconsistent owner metadata or changelog state.
	ErrCorrupt = errors.New("mysql projection: corrupt stored state")
)

type tableSet struct {
	migrations string
	meta       string
	rows       string
	changelog  string
}

// Migration is one ordered, idempotent MySQL schema transition.
type Migration struct {
	Version   uint32
	Statement string
}

// Migrations returns every InnoDB schema transition in ascending order.
func Migrations(prefix string) ([]Migration, error) {
	tables, err := resolveTables(prefix)
	if err != nil {
		return nil, err
	}
	statement := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  version INT UNSIGNED PRIMARY KEY,
  applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  CHECK (version > 0)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS %s (
  projection_id VARCHAR(256) CHARACTER SET ascii PRIMARY KEY,
  fingerprint CHAR(64) CHARACTER SET ascii NOT NULL,
  key_fingerprint CHAR(64) CHARACTER SET ascii NOT NULL,
  head_offset BIGINT UNSIGNED NOT NULL DEFAULT 0,
  floor_offset BIGINT UNSIGNED NOT NULL DEFAULT 0,
  source_time DATETIME(6) NULL,
  storage_version INT UNSIGNED NOT NULL DEFAULT 1,
  CHECK (floor_offset <= head_offset),
  CHECK (storage_version > 0)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS %s (
  projection_id VARCHAR(256) CHARACTER SET ascii NOT NULL,
  row_hash BINARY(32) NOT NULL,
  row_key BLOB NOT NULL,
  payload LONGBLOB NOT NULL,
  PRIMARY KEY (projection_id, row_hash),
  CONSTRAINT %s FOREIGN KEY (projection_id)
    REFERENCES %s(projection_id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS %s (
  projection_id VARCHAR(256) CHARACTER SET ascii NOT NULL,
  offset_value BIGINT UNSIGNED NOT NULL,
  previous_cursor VARCHAR(512) CHARACTER SET ascii NOT NULL,
  cursor_value VARCHAR(512) CHARACTER SET ascii NOT NULL,
  source_time DATETIME(6) NOT NULL,
  change_hash BINARY(32) NOT NULL,
  change_id VARBINARY(512) NOT NULL,
  digest BINARY(32) NOT NULL,
  payload LONGBLOB NULL,
  PRIMARY KEY (projection_id, offset_value),
  UNIQUE KEY %s (projection_id, change_hash),
  CONSTRAINT %s FOREIGN KEY (projection_id)
    REFERENCES %s(projection_id) ON DELETE CASCADE,
  CHECK (offset_value > 0)
) ENGINE=InnoDB;
INSERT IGNORE INTO %s (version) VALUES (1);
`,
		tables.migrations,
		tables.meta,
		tables.rows,
		constraintName(prefix, "rows_meta_fk"),
		tables.meta,
		tables.changelog,
		indexName(prefix, "change_hash_uq"),
		constraintName(prefix, "log_meta_fk"),
		tables.meta,
		tables.migrations,
	)
	return []Migration{{Version: LatestMigrationVersion, Statement: statement}}, nil
}

// Schema returns all idempotent MySQL migrations as one DDL document.
func Schema(prefix string) (string, error) {
	migrations, err := Migrations(prefix)
	if err != nil {
		return "", err
	}
	var document strings.Builder
	for _, migration := range migrations {
		fmt.Fprintf(&document, "-- keelith mysql projection migration %d\n%s", migration.Version, migration.Statement)
	}
	return document.String(), nil
}

func resolveTables(prefix string) (tableSet, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = defaultTablePrefix
	}
	if !validIdentifier(prefix) || len(prefix) > 45 {
		return tableSet{}, fmt.Errorf("%w: table prefix", ErrInvalidOption)
	}
	quoted := func(suffix string) string { return "`" + prefix + suffix + "`" }
	return tableSet{
		migrations: quoted("_migrations"),
		meta:       quoted("_meta"),
		rows:       quoted("_rows"),
		changelog:  quoted("_changelog"),
	}, nil
}

func constraintName(prefix, suffix string) string {
	if strings.TrimSpace(prefix) == "" {
		prefix = defaultTablePrefix
	}
	return "`" + boundedName(prefix+"_"+suffix) + "`"
}

func indexName(prefix, suffix string) string {
	return constraintName(prefix, suffix)
}

func boundedName(value string) string {
	if len(value) <= 64 {
		return value
	}
	digest := sha256.Sum256([]byte(value))
	return fmt.Sprintf("keelith_projection_%x", digest[:12])
}

func validIdentifier(value string) bool {
	if value == "" || !utf8.ValidString(value) {
		return false
	}
	for index, character := range value {
		if character == '_' || unicode.IsLetter(character) || index > 0 && unicode.IsDigit(character) {
			continue
		}
		return false
	}
	return true
}
