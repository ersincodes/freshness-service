from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Union

from pydantic import BaseModel, Field, model_validator


LogicalType = Literal["string", "integer", "float", "date", "boolean"]

TimeGrain = Literal["none", "month", "year", "week"]

AnalyticsOperation = Literal[
    "count_rows",
    "count_distinct",
    "sum",
    "avg",
    "min",
    "max",
    "groupby_count",
    "groupby_sum",
    "groupby_ratio",
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
    order: Literal["count_desc", "count_asc", "value_desc", "value_asc", "key_asc", "key_desc"] = "count_desc"
    top_n: int = 50
    select_columns: list[str] | None = None
    limit: int = 50
    denominator_column: str | None = None
    time_column: str | None = None
    time_grain: TimeGrain = "none"

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
                values["limit"] = 50
            if values.get("filters") is None:
                values["filters"] = []
            if values.get("time_grain") is None:
                values["time_grain"] = "none"
        return values


class ColumnProfile(BaseModel):
    """Per-column statistics."""
    logical_type: str
    null_ratio: float
    distinct_count: int
    min_value: float | int | str | None = None
    max_value: float | int | str | None = None
    dtype: str | None = None
    missing_count: int | None = None
    mean_value: float | None = None
    median_value: float | None = None
    std_value: float | None = None
    top_values: dict[str, int] | None = None


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
    document_id: str | None = None
    sheet_name: str | None = None


@dataclass(frozen=True)
class AnalyticsUnavailable:
    """Scoped analytics cannot run (e.g. no tabular metadata for selected documents)."""

    reason: str
    document_ids: list[str] = field(default_factory=list)
    hint: str = "Select a document that contains tabular data."


@dataclass(frozen=True)
class ForecastUnavailable:
    reason: str
    hint: str = (
        "Upload an Excel file with a date column and numeric measures, then try again."
    )


@dataclass(frozen=True)
class HistoricalPoint:
    date: str
    value: float


class ForecastPlan(BaseModel):
    """LLM-generated plan for on-demand filtered forecasting."""

    document_id: str
    sheet_name: str | None = None
    measure_column: str
    time_column: str | None = None
    filters: list[AnalyticsFilter] = Field(default_factory=list)
    horizon: int = 3
    filter_label: str | None = None
    requested_start: str | None = None
    requested_end: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _coerce_nulls(cls, values: dict) -> dict:
        if isinstance(values, dict):
            if values.get("horizon") is None:
                values["horizon"] = 3
            if values.get("filters") is None:
                values["filters"] = []
        return values


@dataclass(frozen=True)
class ForecastChatPayload:
    """Structured forecast for API / SSE responses."""

    document: str | None
    document_id: str
    sheet: str
    measure: str
    time_column: str
    horizon: int
    point: list[float]
    lower: list[float]
    upper: list[float]
    model: str
    frequency: str = "unknown"
    historical: list[HistoricalPoint] = field(default_factory=list)
    forecast_dates: list[str] = field(default_factory=list)
    filter_label: str | None = None


@dataclass(frozen=True)
class ForecastValidation:
    """Result of validating a forecast against historical data."""

    is_valid: bool
    confidence: Literal["high", "medium", "low"]
    warnings: list[str] = field(default_factory=list)


QueryIntent = Literal["analytics", "forecast", "rag", "cannot_answer"]


class QueryPlan(BaseModel):
    """Unified plan produced by the QueryDecomposer.

    Exactly one of analytics_plan / forecast_plan is populated
    when intent is 'analytics' or 'forecast'. For 'cannot_answer',
    cannot_answer_reason explains why.
    """

    intent: QueryIntent
    reason: str
    analytics_plan: AnalyticsPlan | None = None
    forecast_plan: ForecastPlan | None = None
    cannot_answer_reason: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _coerce_nulls(cls, values: dict) -> dict:
        if isinstance(values, dict):
            if values.get("analytics_plan") is None:
                values["analytics_plan"] = None
            if values.get("forecast_plan") is None:
                values["forecast_plan"] = None
        return values
