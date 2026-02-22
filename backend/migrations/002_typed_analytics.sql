-- Typed analytics: add strict column typing + dataset profiling.
-- Extends document_table_columns with logical/sqlite type metadata.
-- Adds document_table_profiles for dataset-level statistics.

ALTER TABLE document_table_columns ADD COLUMN logical_type TEXT NOT NULL DEFAULT 'string';
ALTER TABLE document_table_columns ADD COLUMN sqlite_type TEXT NOT NULL DEFAULT 'TEXT';
ALTER TABLE document_table_columns ADD COLUMN nullable INTEGER NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS document_table_profiles (
  document_id TEXT NOT NULL,
  sheet_name TEXT NOT NULL,
  row_count INTEGER NOT NULL,
  profile_json TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (document_id, sheet_name)
);
