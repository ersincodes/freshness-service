"""Validate analytics plans, results, and forecasts against metadata and profiles."""
from __future__ import annotations

import logging

from .errors import AnalyticsPlanValidationError
from .models import (
    AnalyticsPlan,
    ColumnMetadata,
    DatasetProfile,
    ForecastChatPayload,
    ForecastValidation,
    DATE_ONLY_OPS,
    NUMERIC_ONLY_OPS,
    STRING_ONLY_OPS,
    UNIVERSAL_OPS,
)

logger = logging.getLogger(__name__)

_OPS_REQUIRING_TARGET = {
    "count_distinct",
    "sum",
    "avg",
    "min",
    "max",
    "groupby_sum",
    "groupby_avg",
    "groupby_ratio",
}
_NUMERIC_AGGREGATES = {"sum", "avg", "groupby_sum", "groupby_avg", "groupby_ratio"}


def validate_plan(
    plan: AnalyticsPlan,
    columns: dict[str, ColumnMetadata],
) -> None:
    """Validate plan structure against known column metadata.

    Raises AnalyticsPlanValidationError on any violation.
    """
    visible_columns = {k: v for k, v in columns.items() if not k.startswith("_")}

    if plan.operation in _OPS_REQUIRING_TARGET and not plan.target_column:
        raise AnalyticsPlanValidationError(
            f"target_column is required for operation '{plan.operation}'"
        )

    if plan.target_column and plan.target_column not in visible_columns:
        raise AnalyticsPlanValidationError(
            f"target_column '{plan.target_column}' not found in columns"
        )

    if plan.operation in _NUMERIC_AGGREGATES and plan.target_column:
        meta = visible_columns[plan.target_column]
        if meta.logical_type not in ("integer", "float"):
            raise AnalyticsPlanValidationError(
                f"Operation '{plan.operation}' requires a numeric column, "
                f"but '{plan.target_column}' is '{meta.logical_type}'"
            )

    if plan.operation == "groupby_ratio" and plan.denominator_column:
        dmeta = visible_columns.get(plan.denominator_column)
        if not dmeta:
            raise AnalyticsPlanValidationError(
                f"denominator_column '{plan.denominator_column}' not found in columns"
            )
        if dmeta.logical_type not in ("integer", "float"):
            raise AnalyticsPlanValidationError(
                f"groupby_ratio denominator '{plan.denominator_column}' must be numeric"
            )

    tg = getattr(plan, "time_grain", "none") or "none"
    if tg != "none" and plan.operation not in ("groupby_sum", "groupby_avg", "groupby_count"):
        raise AnalyticsPlanValidationError(
            "time_grain is only supported for groupby_sum, groupby_avg, and groupby_count"
        )
    if tg != "none" and not plan.time_column:
        raise AnalyticsPlanValidationError("time_grain requires time_column")
    if plan.time_column and plan.time_column not in visible_columns:
        raise AnalyticsPlanValidationError(
            f"time_column '{plan.time_column}' not found in columns"
        )
    if tg != "none" and plan.time_column:
        tc_meta = visible_columns[plan.time_column]
        if tc_meta.logical_type != "date":
            raise AnalyticsPlanValidationError(
                f"time_column '{plan.time_column}' must have logical type date"
            )

    if plan.operation == "groupby_count":
        group_col = plan.group_by or plan.target_column
        if tg == "none" and not group_col:
            raise AnalyticsPlanValidationError(
                "groupby_count requires group_by or target_column (unless using time_grain)"
            )
        if group_col and group_col not in visible_columns:
            raise AnalyticsPlanValidationError(
                f"group_by column '{group_col}' not found in columns"
            )

    if plan.operation in ("groupby_sum", "groupby_avg"):
        if tg == "none" and not plan.group_by:
            raise AnalyticsPlanValidationError(f"{plan.operation} requires group_by")
        if plan.group_by and plan.group_by not in visible_columns:
            raise AnalyticsPlanValidationError(
                f"group_by column '{plan.group_by}' not found in columns"
            )

    if plan.operation == "groupby_ratio":
        if not plan.group_by:
            raise AnalyticsPlanValidationError("groupby_ratio requires group_by")
        if plan.group_by not in visible_columns:
            raise AnalyticsPlanValidationError(
                f"group_by column '{plan.group_by}' not found in columns"
            )
        if not plan.denominator_column:
            raise AnalyticsPlanValidationError("groupby_ratio requires denominator_column")

    if plan.operation == "select_rows" and plan.select_columns:
        for col in plan.select_columns:
            if col not in visible_columns:
                raise AnalyticsPlanValidationError(
                    f"select_columns contains unknown column '{col}'"
                )

    for filt in plan.filters:
        if filt.column not in visible_columns:
            raise AnalyticsPlanValidationError(
                f"Filter column '{filt.column}' not found in columns"
            )
        meta = visible_columns[filt.column]
        _validate_operator_type_compat(filt.operator, meta)


def _validate_operator_type_compat(operator: str, meta: ColumnMetadata) -> None:
    if operator in UNIVERSAL_OPS:
        return

    if operator in NUMERIC_ONLY_OPS:
        if meta.logical_type not in ("integer", "float", "date"):
            raise AnalyticsPlanValidationError(
                f"Operator '{operator}' not valid for "
                f"{meta.logical_type} column '{meta.column_name}'"
            )
        return

    if operator in STRING_ONLY_OPS:
        if meta.logical_type != "string":
            raise AnalyticsPlanValidationError(
                f"Operator '{operator}' not valid for "
                f"{meta.logical_type} column '{meta.column_name}'"
            )
        return

    if operator in DATE_ONLY_OPS:
        if meta.logical_type != "date":
            raise AnalyticsPlanValidationError(
                f"Operator '{operator}' not valid for "
                f"{meta.logical_type} column '{meta.column_name}'"
            )
        return


def validate_result(
    result: dict,
    profile: DatasetProfile | None,
) -> None:
    """Sanity-check the analytics result against the dataset profile.

    Logs warnings rather than raising, since the result is already computed.
    """
    if profile is None:
        return

    if "count" in result:
        count = result["count"]
        if isinstance(count, (int, float)) and count > profile.row_count:
            logger.warning(
                "Result count (%s) exceeds profile row_count (%s)",
                count, profile.row_count,
            )

    if "count_distinct" in result:
        cd = result["count_distinct"]
        if isinstance(cd, (int, float)) and cd > profile.row_count:
            logger.warning(
                "Result count_distinct (%s) exceeds profile row_count (%s)",
                cd, profile.row_count,
            )


# ------------------------------------------------------------------
# Forecast validation
# ------------------------------------------------------------------


def validate_forecast_result(
    payload: ForecastChatPayload,
    expected_periods: int | None = None,
) -> ForecastValidation:
    """Validate a forecast payload for reasonableness.

    Checks:
    - Forecast point count matches expected periods (if provided)
    - Values are within reasonable bounds of historical data
    - No negative values when history is all non-negative
    """
    warnings: list[str] = []

    if expected_periods is not None and len(payload.point) != expected_periods:
        warnings.append(
            f"Expected {expected_periods} forecast points but got {len(payload.point)}."
        )

    hist_values = [h.value for h in payload.historical]
    if hist_values and payload.point:
        hist_max = max(hist_values)
        hist_min = min(hist_values)
        all_positive = all(v >= 0 for v in hist_values)

        for i, val in enumerate(payload.point):
            if all_positive and val < 0:
                warnings.append(
                    f"Forecast period {i+1} is negative ({val:.2f}) "
                    f"but historical values are all non-negative."
                )
            if hist_max > 0 and abs(val) > 3 * hist_max:
                warnings.append(
                    f"Forecast period {i+1} ({val:.2f}) exceeds 3x "
                    f"historical maximum ({hist_max:.2f})."
                )

    if not warnings:
        confidence = "high"
    elif len(warnings) <= 2:
        confidence = "medium"
    else:
        confidence = "low"

    return ForecastValidation(
        is_valid=len(warnings) == 0,
        confidence=confidence,
        warnings=warnings,
    )
