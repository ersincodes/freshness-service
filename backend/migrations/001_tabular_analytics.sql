-- Tabular analytics metadata schema for deterministic spreadsheet queries.
-- Tracks ingested document sheets, their physical SQLite table names,
-- and the column mapping between original Excel headers and safe SQL identifiers.

CREATE TABLE IF NOT EXISTS document_tables (
  document_id TEXT NOT NULL,
  sheet_name TEXT NOT NULL,
  table_name TEXT NOT NULL,
  row_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (document_id, sheet_name),
  UNIQUE (table_name)
);

CREATE INDEX IF NOT EXISTS idx_document_tables_document_id
  ON document_tables(document_id);

CREATE TABLE IF NOT EXISTS document_table_columns (
  document_id TEXT NOT NULL,
  sheet_name TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  original_name TEXT NOT NULL,
  safe_name TEXT NOT NULL,
  inferred_type TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (document_id, sheet_name, ordinal),
  UNIQUE (document_id, sheet_name, safe_name)
);

CREATE INDEX IF NOT EXISTS idx_document_table_columns_doc_sheet
  ON document_table_columns(document_id, sheet_name);

CREATE TABLE IF NOT EXISTS document_default_sheet (
  document_id TEXT NOT NULL PRIMARY KEY,
  sheet_name TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
