package projection

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const defaultTablePrefix = "keelith_projection"

// LatestMigrationVersion is the newest ordered projection storage migration.
const LatestMigrationVersion = 2

var (
	// ErrInvalidOption reports an invalid database, schema, table, or budget.
	ErrInvalidOption = errors.New("postgres projection: invalid option")
	// ErrNotSeeded reports subscription before the first committed change.
	ErrNotSeeded = errors.New("postgres projection: owner is not seeded")
	// ErrSourceClosed reports use after Source.Close.
	ErrSourceClosed = errors.New("postgres projection: source closed")
	// ErrSessionClosed reports Next after Session.Close.
	ErrSessionClosed = errors.New("postgres projection: session closed")
	// ErrInvalidCursor reports a malformed or future PostgreSQL cursor.
	ErrInvalidCursor = errors.New("postgres projection: invalid cursor")
	// ErrCorrupt reports inconsistent owner metadata or changelog state.
	ErrCorrupt = errors.New("postgres projection: corrupt stored state")
)

type tableSet struct {
	migrations string
	meta       string
	rows       string
	changelog  string
}

// Migration is one ordered, idempotent PostgreSQL schema transition.
type Migration struct {
	Version   uint32
	Statement string
}

// Migrations returns every schema transition in ascending version order.
func Migrations(prefix string) ([]Migration, error) {
	tables, err := resolveTables(prefix)
	if err != nil {
		return nil, err
	}
	indexName := changelogIndexName(prefix)
	first := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  version integer PRIMARY KEY CHECK (version > 0),
  applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS %s (
  projection_id varchar(256) PRIMARY KEY,
  fingerprint char(64) NOT NULL,
  key_fingerprint char(64) NOT NULL,
  head_offset bigint NOT NULL DEFAULT 0 CHECK (head_offset >= 0),
  floor_offset bigint NOT NULL DEFAULT 0
    CHECK (floor_offset >= 0 AND floor_offset <= head_offset),
  source_time timestamptz
);
CREATE TABLE IF NOT EXISTS %s (
  projection_id varchar(256) NOT NULL,
  row_key bytea NOT NULL,
  payload bytea NOT NULL,
  PRIMARY KEY (projection_id, row_key)
);
CREATE TABLE IF NOT EXISTS %s (
  projection_id varchar(256) NOT NULL,
  offset_value bigint NOT NULL CHECK (offset_value > 0),
  previous_cursor varchar(512) NOT NULL,
  cursor_value varchar(512) NOT NULL,
  source_time timestamptz NOT NULL,
  change_id varchar(512) NOT NULL,
  digest bytea NOT NULL CHECK (octet_length(digest) = 32),
  payload bytea,
  PRIMARY KEY (projection_id, offset_value),
  UNIQUE (projection_id, change_id)
);
CREATE INDEX IF NOT EXISTS "%s"
  ON %s (projection_id, offset_value)
  WHERE payload IS NOT NULL;
INSERT INTO %s (version) VALUES (1)
ON CONFLICT (version) DO NOTHING;
`,
		tables.migrations,
		tables.meta,
		tables.rows,
		tables.changelog,
		indexName,
		tables.changelog,
		tables.migrations,
	)
	second := fmt.Sprintf(`
ALTER TABLE %s
  ADD COLUMN IF NOT EXISTS storage_version integer NOT NULL DEFAULT 1
    CHECK (storage_version > 0);
INSERT INTO %s (version) VALUES (2)
ON CONFLICT (version) DO NOTHING;
`, tables.meta, tables.migrations)
	return []Migration{
		{Version: 1, Statement: first},
		{Version: LatestMigrationVersion, Statement: second},
	}, nil
}

// Schema returns all versioned, idempotent PostgreSQL migrations as one DDL
// document for owner metadata, current rows, and retained changelog payloads.
func Schema(prefix string) (string, error) {
	migrations, err := Migrations(prefix)
	if err != nil {
		return "", err
	}
	var document strings.Builder
	for _, migration := range migrations {
		fmt.Fprintf(
			&document,
			"-- keelith projection migration %d\n%s",
			migration.Version,
			migration.Statement,
		)
	}
	return document.String(), nil
}

func resolveTables(prefix string) (tableSet, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = defaultTablePrefix
	}
	parts := strings.Split(prefix, ".")
	if len(parts) == 0 || len(parts) > 2 {
		return tableSet{}, fmt.Errorf("%w: table prefix", ErrInvalidOption)
	}
	for _, part := range parts {
		if !validIdentifier(part) {
			return tableSet{}, fmt.Errorf(
				"%w: table identifier",
				ErrInvalidOption,
			)
		}
	}
	schema := ""
	base := parts[len(parts)-1]
	if len(parts) == 2 {
		schema = `"` + parts[0] + `".`
	}
	qualified := func(suffix string) (string, error) {
		name := base + suffix
		if !validIdentifier(name) {
			return "", fmt.Errorf(
				"%w: derived table identifier",
				ErrInvalidOption,
			)
		}
		return schema + `"` + name + `"`, nil
	}
	meta, err := qualified("_meta")
	if err != nil {
		return tableSet{}, err
	}
	migrations, err := qualified("_migrations")
	if err != nil {
		return tableSet{}, err
	}
	rows, err := qualified("_rows")
	if err != nil {
		return tableSet{}, err
	}
	changelog, err := qualified("_changelog")
	if err != nil {
		return tableSet{}, err
	}
	return tableSet{
		migrations: migrations,
		meta:       meta,
		rows:       rows,
		changelog:  changelog,
	}, nil
}

func changelogIndexName(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = defaultTablePrefix
	}
	base := strings.ReplaceAll(prefix, ".", "_") + "_retained_idx"
	if len(base) <= 63 && validIdentifier(base) {
		return base
	}
	sum := sha256.Sum256([]byte(prefix))
	return fmt.Sprintf("keelith_projection_%x_retained_idx", sum[:8])
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 63 || !utf8.ValidString(value) {
		return false
	}
	for index, character := range value {
		if character == '_' ||
			unicode.IsLetter(character) ||
			index > 0 && unicode.IsDigit(character) {
			continue
		}
		return false
	}
	return true
}
