"""Post-ingestion analytics: profiling, time-series metadata, and baseline forecasts."""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

logger = logging.getLogger(__name__)


def run_profiling_and_forecasts(
    conn: sqlite3.Connection,
    meta_repo: Any,
    document_id: str,
    sheet_name: str,
    df: Any,
    col_meta_dict: dict,
) -> None:
    """Compute and persist profile, time-series metadata, and baseline forecasts."""
    try:
        from .profiler import profile_dataframe, build_timeseries_record, measures_json_dumps
        from .forecast_repository import ForecastRepository
        from .forecaster import generate_sheet_forecasts

        profile = profile_dataframe(df, col_meta_dict)
        meta_repo.upsert_profile(document_id, sheet_name, profile)

        tcol, mrows, elig, ts_reason = build_timeseries_record(df, col_meta_dict)
        meta_repo.upsert_timeseries_meta(
            document_id,
            sheet_name,
            tcol,
            measures_json_dumps(mrows),
            elig,
            ts_reason,
        )
        if elig and tcol is not None and mrows:
            fc_repo = ForecastRepository(conn)
            generate_sheet_forecasts(
                df, col_meta_dict, document_id, sheet_name, tcol, mrows, fc_repo,
            )
    except Exception as exc:
        logger.warning("Profiling failed for sheet '%s': %s", sheet_name, exc)
