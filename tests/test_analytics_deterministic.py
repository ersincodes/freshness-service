"""Contract tests for deterministic tabular analytics.

Verifies that:
- Type inference produces correct LogicalTypes
- Date normalization stores epoch integers
- Epoch-based year/month filtering produces exact counts
- The sql_compiler generates correct parameterized SQL
- The validator catches invalid plans
"""
from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from backend.analytics.models import (
    AnalyticsFilter,
    AnalyticsPlan,
    ColumnMetadata,
    DatasetProfile,
)
from backend.analytics.metadata_repository import MetadataRepository
from backend.analytics.profiler import profile_dataframe
from backend.analytics.sql_compiler import (
    CompiledSql,
    compile_between_dates,
    compile_month_equals,
    compile_plan,
    compile_year_equals,
)
from backend.analytics.validator import validate_plan, validate_result
from backend.analytics.errors import AnalyticsPlanValidationError
from backend.documents import _infer_logical_type, _normalize_cell_value


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Build a 10-row DataFrame with known dates spanning 2020–2022."""
    rows = []
    dates = [
        datetime(2020, 3, 15), datetime(2020, 7, 1), datetime(2020, 11, 30),
        datetime(2020, 12, 31),
        datetime(2021, 1, 1), datetime(2021, 6, 15), datetime(2021, 12, 31),
        datetime(2022, 2, 28), datetime(2022, 6, 1), datetime(2022, 12, 15),
    ]
    for i, d in enumerate(dates, 1):
        rows.append({
            "Index": i,
            "Customer Id": f"C{i:04d}",
            "Subscription Date": d,
            "Amount": 100.0 + i,
            "Active": i % 2 == 0,
        })
    return pd.DataFrame(rows)


@pytest.fixture
def in_memory_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS document_tables (
            document_id TEXT NOT NULL,
            sheet_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            row_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (document_id, sheet_name),
            UNIQUE (table_name)
        );
        CREATE TABLE IF NOT EXISTS document_table_columns (
            document_id TEXT NOT NULL,
            sheet_name TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            original_name TEXT NOT NULL,
            safe_name TEXT NOT NULL,
            inferred_type TEXT NOT NULL,
            logical_type TEXT NOT NULL DEFAULT 'string',
            sqlite_type TEXT NOT NULL DEFAULT 'TEXT',
            nullable INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (document_id, sheet_name, ordinal),
            UNIQUE (document_id, sheet_name, safe_name)
        );
        CREATE TABLE IF NOT EXISTS document_default_sheet (
            document_id TEXT NOT NULL PRIMARY KEY,
            sheet_name TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS document_table_profiles (
            document_id TEXT NOT NULL,
            sheet_name TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            profile_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (document_id, sheet_name)
        );
    """)
    yield conn
    conn.close()


# ============================================================================
# Type Inference
# ============================================================================

class TestTypeInference:
    def test_date_column(self):
        s = pd.Series([datetime(2020, 1, 1), datetime(2021, 6, 15), None])
        assert _infer_logical_type(s) == "date"

    def test_integer_column(self):
        s = pd.Series([1, 2, 3, 4])
        assert _infer_logical_type(s) == "integer"

    def test_float_column(self):
        s = pd.Series([1.1, 2.2, 3.3])
        assert _infer_logical_type(s) == "float"

    def test_boolean_column(self):
        s = pd.Series([True, False, True])
        assert _infer_logical_type(s) == "boolean"

    def test_string_column(self):
        s = pd.Series(["hello", "world", "test"])
        assert _infer_logical_type(s) == "string"

    def test_string_dates_detected(self):
        s = pd.Series(["2020-01-15", "2021-06-30", "2022-12-01"])
        assert _infer_logical_type(s) == "date"

    def test_empty_series_defaults_to_string(self):
        s = pd.Series([], dtype=object)
        assert _infer_logical_type(s) == "string"


# ============================================================================
# Cell Normalization
# ============================================================================

class TestCellNormalization:
    def test_date_to_epoch(self):
        d = datetime(2020, 1, 1)
        epoch = _normalize_cell_value(d, "date")
        expected = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp())
        assert epoch == expected

    def test_none_returns_none(self):
        assert _normalize_cell_value(None, "date") is None

    def test_boolean_true(self):
        assert _normalize_cell_value(True, "boolean") == 1

    def test_boolean_false(self):
        assert _normalize_cell_value(False, "boolean") == 0

    def test_integer(self):
        assert _normalize_cell_value("42", "integer") == 42

    def test_float(self):
        assert _normalize_cell_value("3.14", "float") == pytest.approx(3.14)

    def test_string_trim(self):
        assert _normalize_cell_value("  hello  ", "string") == "hello"

    def test_date_string_to_epoch(self):
        epoch = _normalize_cell_value("2020-06-15", "date")
        expected = int(datetime(2020, 6, 15, tzinfo=timezone.utc).timestamp())
        assert epoch == expected


# ============================================================================
# Epoch Compilation Helpers
# ============================================================================

class TestEpochCompilation:
    def test_year_equals_2020(self):
        sql, params = compile_year_equals("col_date", 2020)
        start = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2021, 1, 1, tzinfo=timezone.utc).timestamp())
        assert params == [start, end]
        assert "col_date >= ?" in sql
        assert "col_date < ?" in sql

    def test_month_equals_march_2020(self):
        sql, params = compile_month_equals("col_date", 2020, 3)
        start = int(datetime(2020, 3, 1, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2020, 4, 1, tzinfo=timezone.utc).timestamp())
        assert params == [start, end]

    def test_month_equals_december(self):
        sql, params = compile_month_equals("col_date", 2020, 12)
        start = int(datetime(2020, 12, 1, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2021, 1, 1, tzinfo=timezone.utc).timestamp())
        assert params == [start, end]

    def test_between_dates(self):
        sql, params = compile_between_dates("col_date", "2020-01-01", "2020-12-31")
        start = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp())
        end = int(datetime(2020, 12, 31, tzinfo=timezone.utc).timestamp()) + 86400
        assert params == [start, end]


# ============================================================================
# SQL Compiler Integration
# ============================================================================

class TestSqlCompiler:
    def _col_meta(self) -> dict[str, ColumnMetadata]:
        return {
            "Subscription Date": ColumnMetadata(
                column_name="Subscription Date",
                logical_type="date", sqlite_type="INTEGER", nullable=True,
                original_name="Subscription Date", safe_name="col_subscription_date",
            ),
            "Amount": ColumnMetadata(
                column_name="Amount",
                logical_type="float", sqlite_type="REAL", nullable=True,
                original_name="Amount", safe_name="col_amount",
            ),
            "Customer Id": ColumnMetadata(
                column_name="Customer Id",
                logical_type="string", sqlite_type="TEXT", nullable=True,
                original_name="Customer Id", safe_name="col_customer_id",
            ),
        }

    def test_count_rows_with_year_filter(self):
        plan = AnalyticsPlan(
            document_id="doc1", operation="count_rows",
            filters=[AnalyticsFilter(column="Subscription Date", operator="year_equals", value=2020)],
        )
        result = compile_plan(plan, table_name="t1", column_metadata=self._col_meta())
        assert "COUNT(1)" in result.sql
        assert "col_subscription_date >= ?" in result.sql
        assert len(result.parameters) == 2

    def test_sum_amount(self):
        plan = AnalyticsPlan(
            document_id="doc1", operation="sum", target_column="Amount",
        )
        result = compile_plan(plan, table_name="t1", column_metadata=self._col_meta())
        assert "SUM(col_amount)" in result.sql

    def test_count_distinct(self):
        plan = AnalyticsPlan(
            document_id="doc1", operation="count_distinct", target_column="Customer Id",
        )
        result = compile_plan(plan, table_name="t1", column_metadata=self._col_meta())
        assert "COUNT(DISTINCT col_customer_id)" in result.sql


# ============================================================================
# Validator
# ============================================================================

class TestValidator:
    def _col_meta(self) -> dict[str, ColumnMetadata]:
        return {
            "Subscription Date": ColumnMetadata(
                column_name="Subscription Date",
                logical_type="date", sqlite_type="INTEGER", nullable=True,
                original_name="Subscription Date", safe_name="col_subscription_date",
            ),
            "Amount": ColumnMetadata(
                column_name="Amount",
                logical_type="float", sqlite_type="REAL", nullable=True,
                original_name="Amount", safe_name="col_amount",
            ),
        }

    def test_reject_missing_target_for_sum(self):
        plan = AnalyticsPlan(document_id="doc1", operation="sum")
        with pytest.raises(AnalyticsPlanValidationError):
            validate_plan(plan, self._col_meta())

    def test_reject_string_op_on_date(self):
        plan = AnalyticsPlan(
            document_id="doc1", operation="count_rows",
            filters=[AnalyticsFilter(column="Subscription Date", operator="contains", value="2020")],
        )
        with pytest.raises(AnalyticsPlanValidationError):
            validate_plan(plan, self._col_meta())

    def test_reject_date_op_on_numeric(self):
        plan = AnalyticsPlan(
            document_id="doc1", operation="count_rows",
            filters=[AnalyticsFilter(column="Amount", operator="year_equals", value=2020)],
        )
        with pytest.raises(AnalyticsPlanValidationError):
            validate_plan(plan, self._col_meta())

    def test_valid_plan_passes(self):
        plan = AnalyticsPlan(
            document_id="doc1", operation="count_rows",
            filters=[AnalyticsFilter(column="Subscription Date", operator="year_equals", value=2020)],
        )
        validate_plan(plan, self._col_meta())


# ============================================================================
# Profiler
# ============================================================================

class TestProfiler:
    def test_profile_basic(self, sample_df):
        col_meta = {
            "Index": ColumnMetadata("Index", "integer", "INTEGER", False, "Index", "col_index"),
            "Amount": ColumnMetadata("Amount", "float", "REAL", True, "Amount", "col_amount"),
        }
        df = sample_df.rename(columns={"Index": "col_index", "Amount": "col_amount"})
        profile = profile_dataframe(df, col_meta)
        assert profile.row_count == 10
        assert "Index" in profile.columns
        assert profile.columns["Index"].distinct_count == 10


# ============================================================================
# End-to-End: Ingestion + Query (in-memory)
# ============================================================================

class TestEndToEnd:
    def _ingest_sample(self, conn: sqlite3.Connection, sample_df: pd.DataFrame):
        """Ingest sample_df into an in-memory SQLite with full typed pipeline."""
        from backend.analytics.models import SQLITE_TYPE_MAP
        from backend.analytics.metadata_repository import MetadataRepository

        doc_id = "test-doc"
        sheet = "Sheet1"
        table_name = "test_table"

        meta_repo = MetadataRepository(conn)

        headers = list(sample_df.columns)
        col_types = {}
        for h in headers:
            col_types[h] = _infer_logical_type(sample_df[h])

        safe_names = {h: f"col_{h.lower().replace(' ', '_')}" for h in headers}

        col_meta_list = []
        for h in headers:
            lt = col_types[h]
            col_meta_list.append(ColumnMetadata(
                column_name=h, logical_type=lt,
                sqlite_type=SQLITE_TYPE_MAP[lt], nullable=True,
                original_name=h, safe_name=safe_names[h],
            ))

        cols_ddl = ", ".join(f"{m.safe_name} {m.sqlite_type}" for m in col_meta_list)
        conn.execute(f"DROP TABLE IF EXISTS {table_name};")
        conn.execute(f"CREATE TABLE {table_name} ({cols_ddl});")

        placeholders = ", ".join(["?"] * len(headers))
        safe_cols_sql = ", ".join(m.safe_name for m in col_meta_list)

        for _, row in sample_df.iterrows():
            values = []
            for h in headers:
                values.append(_normalize_cell_value(row[h], col_types[h]))
            conn.execute(f"INSERT INTO {table_name} ({safe_cols_sql}) VALUES ({placeholders});", values)
        conn.commit()

        meta_repo.register_table(doc_id, sheet, table_name, len(sample_df))
        meta_repo.register_columns(doc_id, sheet, col_meta_list)
        meta_repo.register_default_sheet(doc_id, sheet)

        return doc_id, table_name, {m.original_name: m for m in col_meta_list}

    def test_count_by_year(self, in_memory_db, sample_df):
        doc_id, table_name, col_meta = self._ingest_sample(in_memory_db, sample_df)

        # 2020: indices 1-4 → 4 rows
        plan_2020 = AnalyticsPlan(
            document_id=doc_id, operation="count_rows",
            filters=[AnalyticsFilter(column="Subscription Date", operator="year_equals", value=2020)],
        )
        compiled = compile_plan(plan_2020, table_name=table_name, column_metadata=col_meta)
        in_memory_db.row_factory = sqlite3.Row
        rows = in_memory_db.execute(compiled.sql, tuple(compiled.parameters)).fetchall()
        assert rows[0]["count"] == 4

        # 2021: indices 5-7 → 3 rows
        plan_2021 = AnalyticsPlan(
            document_id=doc_id, operation="count_rows",
            filters=[AnalyticsFilter(column="Subscription Date", operator="year_equals", value=2021)],
        )
        compiled = compile_plan(plan_2021, table_name=table_name, column_metadata=col_meta)
        rows = in_memory_db.execute(compiled.sql, tuple(compiled.parameters)).fetchall()
        assert rows[0]["count"] == 3

        # 2022: indices 8-10 → 3 rows
        plan_2022 = AnalyticsPlan(
            document_id=doc_id, operation="count_rows",
            filters=[AnalyticsFilter(column="Subscription Date", operator="year_equals", value=2022)],
        )
        compiled = compile_plan(plan_2022, table_name=table_name, column_metadata=col_meta)
        rows = in_memory_db.execute(compiled.sql, tuple(compiled.parameters)).fetchall()
        assert rows[0]["count"] == 3

    def test_total_count(self, in_memory_db, sample_df):
        doc_id, table_name, col_meta = self._ingest_sample(in_memory_db, sample_df)

        plan_all = AnalyticsPlan(document_id=doc_id, operation="count_rows")
        compiled = compile_plan(plan_all, table_name=table_name, column_metadata=col_meta)
        in_memory_db.row_factory = sqlite3.Row
        rows = in_memory_db.execute(compiled.sql, tuple(compiled.parameters)).fetchall()
        assert rows[0]["count"] == 10

    def test_sum_years_equals_total(self, in_memory_db, sample_df):
        """Key invariant: sum of year counts == total row count."""
        doc_id, table_name, col_meta = self._ingest_sample(in_memory_db, sample_df)
        in_memory_db.row_factory = sqlite3.Row

        year_counts = 0
        for year in (2020, 2021, 2022):
            plan = AnalyticsPlan(
                document_id=doc_id, operation="count_rows",
                filters=[AnalyticsFilter(column="Subscription Date", operator="year_equals", value=year)],
            )
            compiled = compile_plan(plan, table_name=table_name, column_metadata=col_meta)
            rows = in_memory_db.execute(compiled.sql, tuple(compiled.parameters)).fetchall()
            year_counts += rows[0]["count"]

        assert year_counts == 10

    def test_month_filter(self, in_memory_db, sample_df):
        doc_id, table_name, col_meta = self._ingest_sample(in_memory_db, sample_df)
        in_memory_db.row_factory = sqlite3.Row

        plan = AnalyticsPlan(
            document_id=doc_id, operation="count_rows",
            filters=[AnalyticsFilter(column="Subscription Date", operator="month_equals", value="2020-03")],
        )
        compiled = compile_plan(plan, table_name=table_name, column_metadata=col_meta)
        rows = in_memory_db.execute(compiled.sql, tuple(compiled.parameters)).fetchall()
        assert rows[0]["count"] == 1

    def test_between_dates(self, in_memory_db, sample_df):
        doc_id, table_name, col_meta = self._ingest_sample(in_memory_db, sample_df)
        in_memory_db.row_factory = sqlite3.Row

        plan = AnalyticsPlan(
            document_id=doc_id, operation="count_rows",
            filters=[AnalyticsFilter(
                column="Subscription Date", operator="between_dates",
                value=["2020-01-01", "2020-12-31"],
            )],
        )
        compiled = compile_plan(plan, table_name=table_name, column_metadata=col_meta)
        rows = in_memory_db.execute(compiled.sql, tuple(compiled.parameters)).fetchall()
        assert rows[0]["count"] == 4

    def test_boolean_stored_as_int(self, in_memory_db, sample_df):
        doc_id, table_name, col_meta = self._ingest_sample(in_memory_db, sample_df)
        in_memory_db.row_factory = sqlite3.Row

        plan = AnalyticsPlan(
            document_id=doc_id, operation="count_rows",
            filters=[AnalyticsFilter(column="Active", operator="eq", value=1)],
        )
        compiled = compile_plan(plan, table_name=table_name, column_metadata=col_meta)
        rows = in_memory_db.execute(compiled.sql, tuple(compiled.parameters)).fetchall()
        assert rows[0]["count"] == 5

    def test_select_rows_with_filter(self, in_memory_db, sample_df):
        """Test select_rows operation with a country-like filter."""
        doc_id, table_name, col_meta = self._ingest_sample(in_memory_db, sample_df)
        in_memory_db.row_factory = sqlite3.Row

        plan = AnalyticsPlan(
            document_id=doc_id, operation="select_rows",
            select_columns=["Customer Id", "Index"],
            filters=[AnalyticsFilter(column="Active", operator="eq", value=1)],
            limit=10,
        )
        compiled = compile_plan(plan, table_name=table_name, column_metadata=col_meta)
        rows = in_memory_db.execute(compiled.sql, tuple(compiled.parameters)).fetchall()
        assert len(rows) == 5
        assert "Customer Id" in dict(rows[0])

    def test_select_rows_all_columns(self, in_memory_db, sample_df):
        """Test select_rows without select_columns returns all visible columns."""
        doc_id, table_name, col_meta = self._ingest_sample(in_memory_db, sample_df)
        in_memory_db.row_factory = sqlite3.Row

        plan = AnalyticsPlan(
            document_id=doc_id, operation="select_rows",
            filters=[AnalyticsFilter(column="Index", operator="eq", value=1)],
            limit=10,
        )
        compiled = compile_plan(plan, table_name=table_name, column_metadata=col_meta)
        rows = in_memory_db.execute(compiled.sql, tuple(compiled.parameters)).fetchall()
        assert len(rows) == 1
        row_dict = dict(rows[0])
        assert "Index" in row_dict
        assert "Customer Id" in row_dict
        assert "Amount" in row_dict
