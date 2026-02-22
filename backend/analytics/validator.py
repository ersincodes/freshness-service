"""Validate analytics plans and results against metadata and profiles."""
from __future__ import annotations

import logging

from .errors import AnalyticsPlanValidationError
from .models import (
    AnalyticsPlan,
    ColumnMetadata,
    DatasetProfile,
    DATE_ONLY_OPS,
    NUMERIC_ONLY_OPS,
    STRING_ONLY_OPS,
    UNIVERSAL_OPS,
)

logger = logging.getLogger(__name__)

_OPS_REQUIRING_TARGET = {"count_distinct", "sum", "avg", "min", "max"}
_NUMERIC_AGGREGATES = {"sum", "avg"}


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

    if plan.operation == "groupby_count":
        group_col = plan.group_by or plan.target_column
        if not group_col:
            raise AnalyticsPlanValidationError(
                "groupby_count requires group_by or target_column"
            )
        if group_col not in visible_columns:
            raise AnalyticsPlanValidationError(
                f"group_by column '{group_col}' not found in columns"
            )

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
