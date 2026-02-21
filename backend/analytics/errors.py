from __future__ import annotations


class AnalyticsError(Exception):
    """Base error class for deterministic analytics."""


class AnalyticsRoutingError(AnalyticsError):
    """Raised when analytics is selected but required inputs are missing."""


class AnalyticsPlanValidationError(AnalyticsError):
    """Raised when the JSON plan fails Pydantic validation."""


class AnalyticsCompilationError(AnalyticsError):
    """Raised when a validated plan cannot be compiled into safe SQL."""


class AnalyticsExecutionError(AnalyticsError):
    """Raised when SQLite execution fails."""
