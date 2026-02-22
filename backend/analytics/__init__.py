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
    LogicalType,
    AnalyticsOperation,
    AggregationOperation,
    FilterOperator,
    SQLITE_TYPE_MAP,
)
from .router import AnalyticsRouter, AnalyticsRoutingDecision
from .executor import AnalyticsExecutor
from .metadata_repository import MetadataRepository
from .profiler import profile_dataframe
from .validator import validate_plan, validate_result

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
    "LogicalType",
    "AnalyticsOperation",
    "AggregationOperation",
    "FilterOperator",
    "SQLITE_TYPE_MAP",
    "AnalyticsRouter",
    "AnalyticsRoutingDecision",
    "AnalyticsExecutor",
    "MetadataRepository",
    "profile_dataframe",
    "validate_plan",
    "validate_result",
]
