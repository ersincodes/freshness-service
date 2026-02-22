from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Union

from pydantic import BaseModel, Field, model_validator


LogicalType = Literal["string", "integer", "float", "date", "boolean"]

AnalyticsOperation = Literal[
    "count_rows",
    "count_distinct",
    "sum",
    "avg",
    "min",
    "max",
    "groupby_count",
    "select_rows",
]

AggregationOperation = AnalyticsOperation

FilterOperator = Literal[
    "eq", "neq", "gt", "gte", "lt", "lte",
    "contains", "startswith",
    "year_equals", "month_equals", "between_dates",
    "is_null", "is_not_null",
]

NUMERIC_ONLY_OPS: set[str] = {"gt", "gte", "lt", "lte"}
STRING_ONLY_OPS: set[str] = {"contains", "startswith"}
DATE_ONLY_OPS: set[str] = {"year_equals", "month_equals", "between_dates"}
UNIVERSAL_OPS: set[str] = {"eq", "neq", "is_null", "is_not_null"}

SQLITE_TYPE_MAP: dict[str, str] = {
    "string": "TEXT",
    "integer": "INTEGER",
    "float": "REAL",
    "date": "INTEGER",
    "boolean": "INTEGER",
}


@dataclass(frozen=True)
class ColumnMetadata:
    """Typed column descriptor persisted in the registry."""
    column_name: str
    logical_type: LogicalType
    sqlite_type: str
    nullable: bool
    original_name: str
    safe_name: str


class AnalyticsFilter(BaseModel):
    """Semantic filter — the LLM emits these, the backend compiles to SQL."""
    column: str
    operator: FilterOperator
    value: Union[str, int, float, bool, list[Union[str, int, float]], None] = None


class AnalyticsPlan(BaseModel):
    """Flat analytics plan produced by the LLM.

    LLMs frequently emit ``null`` for optional fields instead of omitting them.
    The pre-validator coerces those nulls back to safe defaults so Pydantic's
    Literal/int validators never see None.
    """
    document_id: str
    sheet_name: str | None = None
    operation: AnalyticsOperation
    target_column: str | None = None
    group_by: str | None = None
    filters: list[AnalyticsFilter] = Field(default_factory=list)
    order: Literal["count_desc", "count_asc", "key_asc", "key_desc"] = "count_desc"
    top_n: int = 50
    select_columns: list[str] | None = None
    limit: int = 100

    @model_validator(mode="before")
    @classmethod
    def _coerce_nulls(cls, values: dict) -> dict:
        """Replace explicit nulls from LLM output with safe defaults."""
        if isinstance(values, dict):
            if values.get("order") is None:
                values["order"] = "count_desc"
            if values.get("top_n") is None:
                values["top_n"] = 50
            if values.get("limit") is None:
                values["limit"] = 100
            if values.get("filters") is None:
                values["filters"] = []
        return values


class ColumnProfile(BaseModel):
    """Per-column statistics."""
    logical_type: str
    null_ratio: float
    distinct_count: int
    min_value: float | int | str | None = None
    max_value: float | int | str | None = None


class DatasetProfile(BaseModel):
    """Aggregate statistics for an ingested sheet."""
    row_count: int
    columns: dict[str, ColumnProfile] = Field(default_factory=dict)


class AnalyticsResult(BaseModel):
    """Normalized analytics result returned to ChatService."""
    summary: str
    sql: str
    parameters: list[object]
    data: dict
