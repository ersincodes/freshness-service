from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class ColumnMapping:
    original_name: str
    safe_name: str
    inferred_type: str


class AnalyticsRepository:
    """SQLite access for analytics metadata + query execution."""

    def __init__(self, sqlite_connection: sqlite3.Connection) -> None:
        self._conn = sqlite_connection

    def list_all_document_ids(self) -> list[str]:
        """Return all document IDs that have at least one registered analytics table."""
        cursor = self._conn.execute(
            "SELECT DISTINCT document_id FROM document_tables ORDER BY document_id;",
        )
        return [str(r[0]) for r in cursor.fetchall()]

    def resolve_default_sheet_name(self, document_id: str) -> str | None:
        cursor = self._conn.execute(
            "SELECT sheet_name FROM document_default_sheet WHERE document_id = ? LIMIT 1;",
            (document_id,),
        )
        row = cursor.fetchone()
        return None if row is None else str(row[0])

    def resolve_table_name(self, document_id: str, sheet_name: str) -> str | None:
        cursor = self._conn.execute(
            "SELECT table_name FROM document_tables WHERE document_id = ? AND sheet_name = ? LIMIT 1;",
            (document_id, sheet_name),
        )
        row = cursor.fetchone()
        return None if row is None else str(row[0])

    def fetch_column_mappings(self, document_id: str, sheet_name: str) -> list[ColumnMapping]:
        cursor = self._conn.execute(
            "SELECT original_name, safe_name, inferred_type "
            "FROM document_table_columns "
            "WHERE document_id = ? AND sheet_name = ? "
            "ORDER BY ordinal ASC;",
            (document_id, sheet_name),
        )
        return [ColumnMapping(str(r[0]), str(r[1]), str(r[2])) for r in cursor.fetchall()]

    def execute_parameterized_sql(self, sql: str, parameters: Sequence[object]) -> list[sqlite3.Row]:
        self._conn.row_factory = sqlite3.Row
        cursor = self._conn.execute(sql, tuple(parameters))
        return cursor.fetchall()

    def register_document_sheet_table(
        self,
        *,
        document_id: str,
        sheet_name: str,
        table_name: str,
        row_count: int,
        columns: list[ColumnMapping],
        set_as_default_sheet: bool,
    ) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT INTO document_tables (document_id, sheet_name, table_name, row_count, updated_at) "
                "VALUES (?, ?, ?, ?, datetime('now')) "
                "ON CONFLICT(document_id, sheet_name) "
                "DO UPDATE SET "
                "  table_name = excluded.table_name, "
                "  row_count = excluded.row_count, "
                "  updated_at = datetime('now');",
                (document_id, sheet_name, table_name, int(row_count)),
            )

            self._conn.execute(
                "DELETE FROM document_table_columns WHERE document_id = ? AND sheet_name = ?;",
                (document_id, sheet_name),
            )

            self._conn.executemany(
                "INSERT INTO document_table_columns "
                "(document_id, sheet_name, ordinal, original_name, safe_name, inferred_type) "
                "VALUES (?, ?, ?, ?, ?, ?);",
                [
                    (
                        document_id,
                        sheet_name,
                        i,
                        col.original_name,
                        col.safe_name,
                        col.inferred_type,
                    )
                    for i, col in enumerate(columns)
                ],
            )

            if set_as_default_sheet:
                self._conn.execute(
                    "INSERT INTO document_default_sheet (document_id, sheet_name, updated_at) "
                    "VALUES (?, ?, datetime('now')) "
                    "ON CONFLICT(document_id) "
                    "DO UPDATE SET sheet_name = excluded.sheet_name, updated_at = datetime('now');",
                    (document_id, sheet_name),
                )
