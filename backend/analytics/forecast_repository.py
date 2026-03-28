"""Persistence for upload-time forecast artifacts."""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
import uuid
from dataclasses import dataclass
from typing import Any


PIPELINE_VERSION_FORECAST = "forecast_linear_v2"


@dataclass(frozen=True)
class ForecastArtifactRow:
    id: str
    document_id: str
    sheet_name: str
    measure_column: str
    time_column: str
    forecast: dict[str, Any]
    pipeline_version: str
    generated_at: str


class ForecastRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def save_artifact(
        self,
        *,
        document_id: str,
        sheet_name: str,
        measure_column: str,
        time_column: str,
        forecast: dict[str, Any],
        pipeline_version: str = PIPELINE_VERSION_FORECAST,
    ) -> str:
        fid = str(uuid.uuid4())
        now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
        payload = json.dumps(forecast, sort_keys=True, separators=(",", ":"))
        with self._conn:
            self._conn.execute(
                "INSERT INTO forecast_artifacts "
                "(id, document_id, sheet_name, measure_column, time_column, "
                " forecast_json, pipeline_version, generated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
                (
                    fid,
                    document_id,
                    sheet_name,
                    measure_column,
                    time_column,
                    payload,
                    pipeline_version,
                    now,
                ),
            )
        return fid

    def delete_for_document(self, document_id: str) -> int:
        """Remove all forecast artifacts for a document. Returns rows deleted."""
        with self._conn:
            cur = self._conn.execute(
                "DELETE FROM forecast_artifacts WHERE document_id = ?;",
                (document_id,),
            )
        return cur.rowcount

    def list_outdated_document_ids(
        self, current_version: str = PIPELINE_VERSION_FORECAST
    ) -> list[str]:
        """Return document_ids that have artifacts from an older pipeline version."""
        cur = self._conn.execute(
            "SELECT DISTINCT document_id FROM forecast_artifacts "
            "WHERE pipeline_version != ?;",
            (current_version,),
        )
        return [str(r[0]) for r in cur.fetchall()]

    def list_for_documents(
        self, document_ids: list[str]
    ) -> list[ForecastArtifactRow]:
        if not document_ids:
            return []
        placeholders = ",".join("?" for _ in document_ids)
        cur = self._conn.execute(
            f"SELECT id, document_id, sheet_name, measure_column, time_column, "
            f"forecast_json, pipeline_version, generated_at "
            f"FROM forecast_artifacts WHERE document_id IN ({placeholders}) "
            f"ORDER BY generated_at DESC, document_id, sheet_name, measure_column;",
            tuple(document_ids),
        )
        rows: list[ForecastArtifactRow] = []
        for r in cur.fetchall():
            rows.append(
                ForecastArtifactRow(
                    id=str(r[0]),
                    document_id=str(r[1]),
                    sheet_name=str(r[2]),
                    measure_column=str(r[3]),
                    time_column=str(r[4]),
                    forecast=json.loads(str(r[5])),
                    pipeline_version=str(r[6]),
                    generated_at=str(r[7]),
                )
            )
        return rows
