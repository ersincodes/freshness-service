"""
TTL-based Freshness Logic Module

This module implements source-specific freshness tracking with configurable TTLs.
It provides:
- Pydantic models for source configuration and freshness status
- YAML configuration loading for source definitions
- Freshness calculator comparing LastModifiedDate against TTL
- Fetchers for retrieving LastModifiedDate by source type
"""
from __future__ import annotations

import datetime as dt
import os
import sqlite3
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator


# ============================================================================
# Enums and Constants
# ============================================================================

class SourceType(str, Enum):
    """Supported source types for freshness tracking."""
    POSTGRES = "postgres"
    S3 = "s3"
    API = "api"
    FILE = "file"
    ARCHIVE = "archive"


class FreshnessStatus(str, Enum):
    """Freshness status of a data source."""
    FRESH = "fresh"
    STALE = "stale"
    UNKNOWN = "unknown"
    ERROR = "error"


# ============================================================================
# Pydantic Models - Configuration
# ============================================================================

class PostgresConnection(BaseModel):
    """PostgreSQL connection configuration."""
    host: str = "localhost"
    port: int = 5432
    database: str
    table: str
    timestamp_column: str = "updated_at"
    user: str | None = None
    password: str | None = None


class S3Connection(BaseModel):
    """S3 bucket connection configuration."""
    bucket: str
    prefix: str = ""
    region: str = "us-east-1"
    access_key_id: str | None = None
    secret_access_key: str | None = None


class APIConnection(BaseModel):
    """External API connection configuration."""
    url: str
    method: Literal["HEAD", "GET"] = "HEAD"
    headers: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: int = 10


class FileConnection(BaseModel):
    """Local file connection configuration."""
    path: str


class ArchiveConnection(BaseModel):
    """Archive (SQLite) connection configuration."""
    db_path: str | None = None  # Uses default from settings if not specified


class SourceConfig(BaseModel):
    """Configuration for a single data source."""
    name: str = Field(..., min_length=1, description="Unique source identifier")
    type: SourceType = Field(..., description="Source type")
    ttl_minutes: int = Field(..., ge=1, description="Time-to-live in minutes")
    description: str | None = Field(None, description="Human-readable description")
    connection: dict[str, Any] | None = Field(None, description="Type-specific connection params")
    enabled: bool = Field(True, description="Whether this source is active")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Ensure name is a valid identifier."""
        if not v.replace("_", "").replace("-", "").isalnum():
            raise ValueError("Source name must be alphanumeric with underscores/hyphens only")
        return v.lower()


class SourcesConfig(BaseModel):
    """Root configuration containing all source definitions."""
    sources: list[SourceConfig] = Field(default_factory=list)


# ============================================================================
# Pydantic Models - Freshness Reports
# ============================================================================

class FreshnessDetail(BaseModel):
    """Detailed freshness information for a single source."""
    source_name: str = Field(..., description="Source identifier")
    source_type: SourceType = Field(..., description="Type of data source")
    status: FreshnessStatus = Field(..., description="Current freshness status")
    ttl_minutes: int = Field(..., description="Configured TTL in minutes")
    last_modified: str | None = Field(None, description="ISO timestamp of last modification")
    age_minutes: float | None = Field(None, description="Age of data in minutes")
    age_seconds: float | None = Field(None, description="Age of data in seconds")
    time_until_stale_minutes: float | None = Field(
        None, description="Minutes until data becomes stale (negative if already stale)"
    )
    time_until_stale_seconds: float | None = Field(
        None, description="Seconds until data becomes stale (negative if already stale)"
    )
    description: str | None = Field(None, description="Source description")
    error_message: str | None = Field(None, description="Error details if status is ERROR")
    checked_at: str = Field(..., description="ISO timestamp when freshness was checked")


class FreshnessOverview(BaseModel):
    """Summary of freshness across all sources."""
    total_sources: int = Field(..., description="Total number of configured sources")
    fresh_count: int = Field(..., description="Number of fresh sources")
    stale_count: int = Field(..., description="Number of stale sources")
    error_count: int = Field(..., description="Number of sources with errors")
    unknown_count: int = Field(..., description="Number of sources with unknown status")
    overall_status: FreshnessStatus = Field(
        ..., description="Overall system freshness (stale if any source is stale)"
    )


class FreshnessReportResponse(BaseModel):
    """Complete freshness report for all sources."""
    overview: FreshnessOverview
    sources: list[FreshnessDetail]
    checked_at: str = Field(..., description="ISO timestamp of the report")


class SingleSourceFreshnessResponse(BaseModel):
    """Freshness response for a single source."""
    detail: FreshnessDetail
    is_fresh: bool = Field(..., description="Quick boolean check")


# ============================================================================
# Configuration Loading
# ============================================================================

_CONFIG_CACHE: SourcesConfig | None = None
_CONFIG_MTIME: float = 0.0


def _get_config_path() -> Path:
    """Get the path to sources.yaml configuration file."""
    return Path(__file__).parent / "sources.yaml"


def load_sources_config(force_reload: bool = False) -> SourcesConfig:
    """
    Load source configurations from sources.yaml.
    
    Caches the configuration and reloads only if the file has been modified.
    
    Args:
        force_reload: Force reload even if cache is valid
        
    Returns:
        SourcesConfig with all source definitions
    """
    global _CONFIG_CACHE, _CONFIG_MTIME
    
    config_path = _get_config_path()
    
    if not config_path.exists():
        return SourcesConfig(sources=[])
    
    current_mtime = config_path.stat().st_mtime
    
    if not force_reload and _CONFIG_CACHE is not None and current_mtime == _CONFIG_MTIME:
        return _CONFIG_CACHE
    
    with open(config_path, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f) or {}
    
    _CONFIG_CACHE = SourcesConfig(**raw_config)
    _CONFIG_MTIME = current_mtime
    
    return _CONFIG_CACHE


def get_source_by_name(name: str) -> SourceConfig | None:
    """
    Get a specific source configuration by name.
    
    Args:
        name: Source identifier
        
    Returns:
        SourceConfig if found, None otherwise
    """
    config = load_sources_config()
    name_lower = name.lower()
    for source in config.sources:
        if source.name == name_lower:
            return source
    return None


def get_enabled_sources() -> list[SourceConfig]:
    """Get all enabled source configurations."""
    config = load_sources_config()
    return [s for s in config.sources if s.enabled]


# ============================================================================
# Freshness Calculator
# ============================================================================

@dataclass(frozen=True)
class FreshnessResult:
    """Result of a freshness calculation."""
    status: FreshnessStatus
    age_seconds: float | None
    time_until_stale_seconds: float | None
    last_modified: dt.datetime | None
    error_message: str | None = None


def calculate_freshness(
    last_modified: dt.datetime | None,
    ttl_minutes: int,
    reference_time: dt.datetime | None = None,
) -> FreshnessResult:
    """
    Calculate freshness status by comparing LastModifiedDate against TTL.
    
    This function handles timezone-aware timestamps correctly to avoid
    false "stale" alerts due to UTC offsets.
    
    Args:
        last_modified: Timestamp of last data modification (should be UTC)
        ttl_minutes: Time-to-live threshold in minutes
        reference_time: Reference time for comparison (defaults to UTC now)
        
    Returns:
        FreshnessResult with status, age, and time until stale
    """
    if last_modified is None:
        return FreshnessResult(
            status=FreshnessStatus.UNKNOWN,
            age_seconds=None,
            time_until_stale_seconds=None,
            last_modified=None,
            error_message="No last modified timestamp available",
        )
    
    # Normalize to UTC for consistent comparison
    if reference_time is None:
        reference_time = dt.datetime.utcnow()
    
    # Handle timezone-aware timestamps
    if last_modified.tzinfo is not None:
        # Convert to naive UTC for comparison
        last_modified = last_modified.replace(tzinfo=None)
        if reference_time.tzinfo is not None:
            reference_time = reference_time.replace(tzinfo=None)
    
    # Calculate age
    delta = reference_time - last_modified
    age_seconds = delta.total_seconds()
    
    # Handle future timestamps (clock skew)
    if age_seconds < 0:
        return FreshnessResult(
            status=FreshnessStatus.FRESH,
            age_seconds=0.0,
            time_until_stale_seconds=ttl_minutes * 60,
            last_modified=last_modified,
            error_message="Last modified timestamp is in the future (clock skew)",
        )
    
    # Calculate TTL threshold
    ttl_seconds = ttl_minutes * 60
    time_until_stale = ttl_seconds - age_seconds
    
    # Determine status
    if age_seconds <= ttl_seconds:
        status = FreshnessStatus.FRESH
    else:
        status = FreshnessStatus.STALE
    
    return FreshnessResult(
        status=status,
        age_seconds=age_seconds,
        time_until_stale_seconds=time_until_stale,
        last_modified=last_modified,
    )


# ============================================================================
# LastModifiedDate Fetchers
# ============================================================================

def fetch_archive_last_modified(db_path: str) -> dt.datetime | None:
    """
    Fetch the most recent timestamp from the archive database.
    
    Args:
        db_path: Path to SQLite database
        
    Returns:
        Most recent page timestamp, or None if archive is empty
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT MAX(timestamp) FROM pages")
            row = cur.fetchone()
            if row and row[0]:
                # Parse the timestamp string
                timestamp_str = row[0]
                if isinstance(timestamp_str, str):
                    # Handle various datetime formats
                    for fmt in [
                        "%Y-%m-%d %H:%M:%S.%f",
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%dT%H:%M:%S.%f",
                        "%Y-%m-%dT%H:%M:%S",
                    ]:
                        try:
                            return dt.datetime.strptime(timestamp_str, fmt)
                        except ValueError:
                            continue
                elif isinstance(timestamp_str, dt.datetime):
                    return timestamp_str
            return None
    except Exception:
        return None


def fetch_file_last_modified(file_path: str) -> dt.datetime | None:
    """
    Fetch the last modified timestamp of a local file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        File modification timestamp, or None if file doesn't exist
    """
    try:
        path = Path(file_path)
        if path.exists():
            mtime = path.stat().st_mtime
            return dt.datetime.utcfromtimestamp(mtime)
        return None
    except Exception:
        return None


def fetch_api_last_modified(
    url: str,
    method: str = "HEAD",
    headers: dict[str, str] | None = None,
    timeout: int = 10,
) -> dt.datetime | None:
    """
    Fetch the Last-Modified header from an API endpoint.
    
    Args:
        url: API endpoint URL
        method: HTTP method (HEAD or GET)
        headers: Optional request headers
        timeout: Request timeout in seconds
        
    Returns:
        Parsed Last-Modified timestamp, or None if not available
    """
    import requests
    from email.utils import parsedate_to_datetime
    
    try:
        if method.upper() == "HEAD":
            response = requests.head(url, headers=headers or {}, timeout=timeout)
        else:
            response = requests.get(url, headers=headers or {}, timeout=timeout)
        
        response.raise_for_status()
        
        # Check Last-Modified header
        last_modified_str = response.headers.get("Last-Modified")
        if last_modified_str:
            return parsedate_to_datetime(last_modified_str).replace(tzinfo=None)
        
        # Fallback to Date header
        date_str = response.headers.get("Date")
        if date_str:
            return parsedate_to_datetime(date_str).replace(tzinfo=None)
        
        return None
    except Exception:
        return None


def fetch_last_modified_for_source(
    source: SourceConfig,
    default_db_path: str | None = None,
) -> tuple[dt.datetime | None, str | None]:
    """
    Fetch the LastModifiedDate for a source based on its type.
    
    Args:
        source: Source configuration
        default_db_path: Default database path for archive sources
        
    Returns:
        Tuple of (timestamp, error_message)
    """
    try:
        if source.type == SourceType.ARCHIVE:
            conn = source.connection or {}
            db_path = conn.get("db_path") or default_db_path
            if not db_path:
                return None, "No database path configured for archive source"
            return fetch_archive_last_modified(db_path), None
        
        elif source.type == SourceType.FILE:
            if not source.connection:
                return None, "No connection configuration for file source"
            file_conn = FileConnection(**source.connection)
            return fetch_file_last_modified(file_conn.path), None
        
        elif source.type == SourceType.API:
            if not source.connection:
                return None, "No connection configuration for API source"
            api_conn = APIConnection(**source.connection)
            # Expand environment variables in headers
            expanded_headers = {}
            for key, value in api_conn.headers.items():
                if value.startswith("${") and value.endswith("}"):
                    env_var = value[2:-1]
                    expanded_headers[key] = os.getenv(env_var, "")
                else:
                    expanded_headers[key] = value
            return fetch_api_last_modified(
                api_conn.url,
                api_conn.method,
                expanded_headers,
                api_conn.timeout_seconds,
            ), None
        
        elif source.type == SourceType.POSTGRES:
            # PostgreSQL fetcher would require psycopg2 or asyncpg
            # Returning placeholder for now
            return None, "PostgreSQL source type not yet implemented"
        
        elif source.type == SourceType.S3:
            # S3 fetcher would require boto3
            # Returning placeholder for now
            return None, "S3 source type not yet implemented"
        
        else:
            return None, f"Unknown source type: {source.type}"
    
    except Exception as e:
        return None, str(e)


# ============================================================================
# High-Level Freshness Check Functions
# ============================================================================

def check_source_freshness(
    source: SourceConfig,
    default_db_path: str | None = None,
) -> FreshnessDetail:
    """
    Check freshness for a single source.
    
    Args:
        source: Source configuration
        default_db_path: Default database path for archive sources
        
    Returns:
        FreshnessDetail with complete freshness information
    """
    checked_at = dt.datetime.utcnow()
    
    # Fetch last modified timestamp
    last_modified, fetch_error = fetch_last_modified_for_source(source, default_db_path)
    
    if fetch_error and last_modified is None:
        return FreshnessDetail(
            source_name=source.name,
            source_type=source.type,
            status=FreshnessStatus.ERROR,
            ttl_minutes=source.ttl_minutes,
            last_modified=None,
            age_minutes=None,
            age_seconds=None,
            time_until_stale_minutes=None,
            time_until_stale_seconds=None,
            description=source.description,
            error_message=fetch_error,
            checked_at=checked_at.isoformat(),
        )
    
    # Calculate freshness
    result = calculate_freshness(last_modified, source.ttl_minutes, checked_at)
    
    return FreshnessDetail(
        source_name=source.name,
        source_type=source.type,
        status=result.status,
        ttl_minutes=source.ttl_minutes,
        last_modified=result.last_modified.isoformat() if result.last_modified else None,
        age_minutes=round(result.age_seconds / 60, 2) if result.age_seconds is not None else None,
        age_seconds=round(result.age_seconds, 2) if result.age_seconds is not None else None,
        time_until_stale_minutes=(
            round(result.time_until_stale_seconds / 60, 2)
            if result.time_until_stale_seconds is not None
            else None
        ),
        time_until_stale_seconds=(
            round(result.time_until_stale_seconds, 2)
            if result.time_until_stale_seconds is not None
            else None
        ),
        description=source.description,
        error_message=result.error_message or fetch_error,
        checked_at=checked_at.isoformat(),
    )


def check_all_sources_freshness(
    default_db_path: str | None = None,
) -> FreshnessReportResponse:
    """
    Check freshness for all enabled sources.
    
    Args:
        default_db_path: Default database path for archive sources
        
    Returns:
        FreshnessReportResponse with overview and details for all sources
    """
    checked_at = dt.datetime.utcnow()
    sources = get_enabled_sources()
    
    details: list[FreshnessDetail] = []
    fresh_count = 0
    stale_count = 0
    error_count = 0
    unknown_count = 0
    
    for source in sources:
        detail = check_source_freshness(source, default_db_path)
        details.append(detail)
        
        if detail.status == FreshnessStatus.FRESH:
            fresh_count += 1
        elif detail.status == FreshnessStatus.STALE:
            stale_count += 1
        elif detail.status == FreshnessStatus.ERROR:
            error_count += 1
        else:
            unknown_count += 1
    
    # Determine overall status
    if stale_count > 0:
        overall_status = FreshnessStatus.STALE
    elif error_count > 0:
        overall_status = FreshnessStatus.ERROR
    elif unknown_count > 0 and fresh_count == 0:
        overall_status = FreshnessStatus.UNKNOWN
    elif fresh_count > 0:
        overall_status = FreshnessStatus.FRESH
    else:
        overall_status = FreshnessStatus.UNKNOWN
    
    overview = FreshnessOverview(
        total_sources=len(sources),
        fresh_count=fresh_count,
        stale_count=stale_count,
        error_count=error_count,
        unknown_count=unknown_count,
        overall_status=overall_status,
    )
    
    return FreshnessReportResponse(
        overview=overview,
        sources=details,
        checked_at=checked_at.isoformat(),
    )
