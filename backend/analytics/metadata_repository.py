"""Registry for typed column metadata and dataset profiles."""
from __future__ import annotations

import json
import sqlite3
from typing import Sequence

from .models import ColumnMetadata, DatasetProfile


class MetadataRepository:
    """Read/write column metadata and dataset profiles from SQLite registry tables."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    # ------------------------------------------------------------------
    # Table registry
    # ------------------------------------------------------------------

    def register_table(
        self,
        document_id: str,
        sheet_name: str,
        table_name: str,
        row_count: int,
    ) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT INTO document_tables (document_id, sheet_name, table_name, row_count, updated_at) "
                "VALUES (?, ?, ?, ?, datetime('now')) "
                "ON CONFLICT(document_id, sheet_name) "
                "DO UPDATE SET table_name = excluded.table_name, "
                "  row_count = excluded.row_count, updated_at = datetime('now');",
                (document_id, sheet_name, table_name, row_count),
            )

    def register_default_sheet(self, document_id: str, sheet_name: str) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT INTO document_default_sheet (document_id, sheet_name, updated_at) "
                "VALUES (?, ?, datetime('now')) "
                "ON CONFLICT(document_id) DO UPDATE SET "
                "  sheet_name = excluded.sheet_name, updated_at = datetime('now');",
                (document_id, sheet_name),
            )

    def get_table_name(self, document_id: str, sheet_name: str | None) -> str | None:
        if sheet_name is None:
            sheet_name = self._resolve_default_sheet(document_id)
        if sheet_name is None:
            return None
        cur = self._conn.execute(
            "SELECT table_name FROM document_tables "
            "WHERE document_id = ? AND sheet_name = ? LIMIT 1;",
            (document_id, sheet_name),
        )
        row = cur.fetchone()
        return str(row[0]) if row else None

    # ------------------------------------------------------------------
    # Column registry
    # ------------------------------------------------------------------

    def register_columns(
        self,
        document_id: str,
        sheet_name: str,
        columns: Sequence[ColumnMetadata],
    ) -> None:
        with self._conn:
            self._conn.execute(
                "DELETE FROM document_table_columns "
                "WHERE document_id = ? AND sheet_name = ?;",
                (document_id, sheet_name),
            )
            self._conn.executemany(
                "INSERT INTO document_table_columns "
                "(document_id, sheet_name, ordinal, original_name, safe_name, "
                " inferred_type, logical_type, sqlite_type, nullable) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                [
                    (
                        document_id,
                        sheet_name,
                        i,
                        col.original_name,
                        col.safe_name,
                        col.logical_type,
                        col.logical_type,
                        col.sqlite_type,
                        1 if col.nullable else 0,
                    )
                    for i, col in enumerate(columns)
                ],
            )

    def get_columns(
        self, document_id: str, sheet_name: str | None
    ) -> dict[str, ColumnMetadata]:
        if sheet_name is None:
            sheet_name = self._resolve_default_sheet(document_id)
        if sheet_name is None:
            return {}
        cur = self._conn.execute(
            "SELECT original_name, safe_name, logical_type, sqlite_type, nullable "
            "FROM document_table_columns "
            "WHERE document_id = ? AND sheet_name = ? "
            "ORDER BY ordinal ASC;",
            (document_id, sheet_name),
        )
        result: dict[str, ColumnMetadata] = {}
        for row in cur.fetchall():
            original_name = str(row[0])
            result[original_name] = ColumnMetadata(
                column_name=original_name,
                logical_type=str(row[2]) or "string",
                sqlite_type=str(row[3]) or "TEXT",
                nullable=bool(row[4]),
                original_name=original_name,
                safe_name=str(row[1]),
            )
        return result

    # ------------------------------------------------------------------
    # Profiles
    # ------------------------------------------------------------------

    def upsert_profile(
        self, document_id: str, sheet_name: str, profile: DatasetProfile
    ) -> None:
        profile_json = profile.model_dump_json()
        with self._conn:
            self._conn.execute(
                "INSERT INTO document_table_profiles "
                "(document_id, sheet_name, row_count, profile_json, updated_at) "
                "VALUES (?, ?, ?, ?, datetime('now')) "
                "ON CONFLICT(document_id, sheet_name) "
                "DO UPDATE SET row_count = excluded.row_count, "
                "  profile_json = excluded.profile_json, updated_at = datetime('now');",
                (document_id, sheet_name, profile.row_count, profile_json),
            )

    def get_profile(
        self, document_id: str, sheet_name: str | None
    ) -> DatasetProfile | None:
        if sheet_name is None:
            sheet_name = self._resolve_default_sheet(document_id)
        if sheet_name is None:
            return None
        cur = self._conn.execute(
            "SELECT profile_json FROM document_table_profiles "
            "WHERE document_id = ? AND sheet_name = ? LIMIT 1;",
            (document_id, sheet_name),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return DatasetProfile.model_validate_json(row[0])

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_default_sheet(self, document_id: str) -> str | None:
        cur = self._conn.execute(
            "SELECT sheet_name FROM document_default_sheet "
            "WHERE document_id = ? LIMIT 1;",
            (document_id,),
        )
        row = cur.fetchone()
        return str(row[0]) if row else None

    def list_all_document_ids(self) -> list[str]:
        """Return document IDs that exist in both analytics metadata and the documents table."""
        cur = self._conn.execute(
            "SELECT DISTINCT dt.document_id FROM document_tables dt "
            "INNER JOIN documents d ON dt.document_id = d.document_id "
            "WHERE d.status = 'ready' "
            "ORDER BY dt.document_id;"
        )
        return [str(r[0]) for r in cur.fetchall()]

    def delete_document(self, document_id: str) -> None:
        """Remove all analytics metadata and data tables for a document."""
        table_names = []
        cur = self._conn.execute(
            "SELECT table_name FROM document_tables WHERE document_id = ?;",
            (document_id,),
        )
        table_names = [str(r[0]) for r in cur.fetchall()]

        with self._conn:
            for tn in table_names:
                self._conn.execute(f"DROP TABLE IF EXISTS [{tn}];")
            self._conn.execute(
                "DELETE FROM document_table_columns WHERE document_id = ?;",
                (document_id,),
            )
            self._conn.execute(
                "DELETE FROM document_table_profiles WHERE document_id = ?;",
                (document_id,),
            )
            self._conn.execute(
                "DELETE FROM document_default_sheet WHERE document_id = ?;",
                (document_id,),
            )
            self._conn.execute(
                "DELETE FROM document_tables WHERE document_id = ?;",
                (document_id,),
            )

    def resolve_default_sheet_name(self, document_id: str) -> str | None:
        return self._resolve_default_sheet(document_id)
