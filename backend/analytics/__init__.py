"""Deterministic tabular analytics module for spreadsheet aggregation queries."""
from .errors import (
    AnalyticsError,
    AnalyticsRoutingError,
    AnalyticsPlanValidationError,
    AnalyticsCompilationError,
    AnalyticsExecutionError,
)
from .models import (
    AnalyticsPlan,
    AnalyticsResult,
    AnalyticsFilter,
    ColumnMetadata,
    ColumnProfile,
    DatasetProfile,
    ForecastValidation,
    LogicalType,
    AnalyticsOperation,
    AggregationOperation,
    FilterOperator,
    QueryIntent,
    QueryPlan,
    SQLITE_TYPE_MAP,
    TimeGrain,
)
from .router import AnalyticsRouter, AnalyticsRoutingDecision
from .executor import AnalyticsExecutor
from .metadata_repository import MetadataRepository
from .profiler import profile_dataframe
from .query_decomposer import QueryDecomposer
from .standardizer import DataStandardizer
from .dataset_summary import DatasetSummary, build_dataset_summary
from .validator import validate_plan, validate_result, validate_forecast_result

__all__ = [
    "AnalyticsError",
    "AnalyticsRoutingError",
    "AnalyticsPlanValidationError",
    "AnalyticsCompilationError",
    "AnalyticsExecutionError",
    "AnalyticsPlan",
    "AnalyticsResult",
    "AnalyticsFilter",
    "ColumnMetadata",
    "ColumnProfile",
    "DatasetProfile",
    "DatasetSummary",
    "DataStandardizer",
    "ForecastValidation",
    "LogicalType",
    "AnalyticsOperation",
    "AggregationOperation",
    "FilterOperator",
    "QueryDecomposer",
    "QueryIntent",
    "QueryPlan",
    "SQLITE_TYPE_MAP",
    "TimeGrain",
    "AnalyticsRouter",
    "AnalyticsRoutingDecision",
    "AnalyticsExecutor",
    "MetadataRepository",
    "build_dataset_summary",
    "profile_dataframe",
    "validate_plan",
    "validate_result",
    "validate_forecast_result",
]
