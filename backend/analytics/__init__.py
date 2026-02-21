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
    CountRowsPlan,
    CountDistinctPlan,
    GroupByCountPlan,
    TableRef,
    WherePredicate,
)
from .router import AnalyticsRouter, AnalyticsRoutingDecision
from .executor import AnalyticsExecutor

__all__ = [
    "AnalyticsError",
    "AnalyticsRoutingError",
    "AnalyticsPlanValidationError",
    "AnalyticsCompilationError",
    "AnalyticsExecutionError",
    "AnalyticsPlan",
    "AnalyticsResult",
    "CountRowsPlan",
    "CountDistinctPlan",
    "GroupByCountPlan",
    "TableRef",
    "WherePredicate",
    "AnalyticsRouter",
    "AnalyticsRoutingDecision",
    "AnalyticsExecutor",
]
