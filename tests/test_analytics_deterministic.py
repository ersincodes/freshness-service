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
    ColumnProfile,
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
from backend.analytics.filter_value_normalizer import normalize_analytics_plan_filters
from backend.analytics.router import AnalyticsRouter
from backend.analytics.validator import validate_plan, validate_result
from backend.analytics.errors import AnalyticsPlanValidationError
from backend.analytics.standardizer import infer_logical_type, normalize_cell
from backend.services.chat_service import (
    apply_select_rows_limit_from_user_query,
    infer_select_rows_limit_from_query,
    _repair_rowcount_plan_to_quantity_sum,
    _repair_select_rows_to_groupby_superlative,
)


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
        assert infer_logical_type(s) == "date"

    def test_integer_column(self):
        s = pd.Series([1, 2, 3, 4])
        assert infer_logical_type(s) == "integer"

    def test_float_column(self):
        s = pd.Series([1.1, 2.2, 3.3])
        assert infer_logical_type(s) == "float"

    def test_boolean_column(self):
        s = pd.Series([True, False, True])
        assert infer_logical_type(s) == "boolean"

    def test_string_column(self):
        s = pd.Series(["hello", "world", "test"])
        assert infer_logical_type(s) == "string"

    def test_string_dates_detected(self):
        s = pd.Series(["2020-01-15", "2021-06-30", "2022-12-01"])
        assert infer_logical_type(s) == "date"

    def test_empty_series_defaults_to_string(self):
        s = pd.Series([], dtype=object)
        assert infer_logical_type(s) == "string"

    def test_currency_strings_infer_as_float(self):
        s = pd.Series(["$1,234.56", "$2,000.00", "(100.25)", "€500"])
        assert infer_logical_type(s) == "float"

    def test_currency_strings_infer_as_integer_when_whole_units(self):
        s = pd.Series([f"${i:,}" for i in range(1, 11)])
        assert infer_logical_type(s) == "integer"


# ============================================================================
# Cell Normalization
# ============================================================================

class TestCellNormalization:
    def test_date_to_epoch(self):
        d = datetime(2020, 1, 1)
        epoch = normalize_cell(d, "date")
        expected = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp())
        assert epoch == expected

    def test_none_returns_none(self):
        assert normalize_cell(None, "date") is None

    def test_boolean_true(self):
        assert normalize_cell(True, "boolean") == 1

    def test_boolean_false(self):
        assert normalize_cell(False, "boolean") == 0

    def test_integer(self):
        assert normalize_cell("42", "integer") == 42

    def test_float(self):
        assert normalize_cell("3.14", "float") == pytest.approx(3.14)

    def test_string_trim(self):
        assert normalize_cell("  hello  ", "string") == "hello"

    def test_date_string_to_epoch(self):
        epoch = normalize_cell("2020-06-15", "date")
        expected = int(datetime(2020, 6, 15, tzinfo=timezone.utc).timestamp())
        assert epoch == expected

    def test_currency_string_normalizes_to_float(self):
        assert normalize_cell("$1,234.50", "float") == pytest.approx(1234.5)

    def test_accounting_negative_parentheses_to_float(self):
        assert normalize_cell("(99.5)", "float") == pytest.approx(-99.5)


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
            "Country": ColumnMetadata(
                column_name="Country",
                logical_type="string", sqlite_type="TEXT", nullable=True,
                original_name="Country", safe_name="col_country",
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

    def test_groupby_count_sql(self):
        plan = AnalyticsPlan(
            document_id="doc1",
            operation="groupby_count",
            group_by="Country",
            order="count_desc",
            top_n=1,
        )
        result = compile_plan(plan, table_name="t1", column_metadata=self._col_meta())
        assert "COUNT(1) AS cnt" in result.sql
        assert "GROUP BY col_country" in result.sql
        assert "ORDER BY cnt DESC" in result.sql
        assert "LIMIT 1" in result.sql

    def test_groupby_sum_sql(self):
        plan = AnalyticsPlan(
            document_id="doc1",
            operation="groupby_sum",
            target_column="Amount",
            group_by="Country",
            order="value_desc",
            top_n=1,
        )
        result = compile_plan(plan, table_name="t1", column_metadata=self._col_meta())
        assert "SUM(col_amount) AS value" in result.sql
        assert "GROUP BY col_country" in result.sql
        assert "ORDER BY value DESC" in result.sql
        assert "LIMIT 1" in result.sql

    def test_select_rows_orders_by_source_row_number_when_present(self):
        meta = {
            **self._col_meta(),
            "_source_row_number": ColumnMetadata(
                column_name="_source_row_number",
                logical_type="integer",
                sqlite_type="INTEGER",
                nullable=False,
                original_name="_source_row_number",
                safe_name="col__source_row_number",
            ),
        }
        plan = AnalyticsPlan(document_id="doc1", operation="select_rows", limit=3)
        result = compile_plan(plan, table_name="t1", column_metadata=meta)
        assert "ORDER BY col__source_row_number ASC" in result.sql
        assert "LIMIT 3" in result.sql

    def test_select_rows_no_order_without_source_row_column(self):
        plan = AnalyticsPlan(document_id="doc1", operation="select_rows", limit=5)
        result = compile_plan(plan, table_name="t1", column_metadata=self._col_meta())
        assert "ORDER BY" not in result.sql
        assert "LIMIT 5" in result.sql


# ============================================================================
# select_rows limit inference (user query)
# ============================================================================


class TestSelectRowsLimitFromQuery:
    def test_first_ten_over_filename_hundred(self):
        q = "List first 10 rows of 100 Sales Record file ?"
        assert infer_select_rows_limit_from_query(q) == 10

    def test_top_five(self):
        assert infer_select_rows_limit_from_query("show top 5 customers") == 5

    def test_num_rows_phrase(self):
        assert infer_select_rows_limit_from_query("list 20 rows") == 20

    def test_limit_keyword(self):
        assert infer_select_rows_limit_from_query("select all limit 7") == 7

    def test_no_match_for_filename_only(self):
        assert infer_select_rows_limit_from_query("open 100 Sales Record file") is None

    def test_apply_override_only_for_select_rows(self):
        plan = AnalyticsPlan(document_id="d", operation="count_rows")
        assert apply_select_rows_limit_from_user_query(plan, "first 3 rows") is plan

    def test_apply_override_sets_limit(self):
        plan = AnalyticsPlan(document_id="d", operation="select_rows", limit=100)
        out = apply_select_rows_limit_from_user_query(
            plan, "List first 10 rows of 100 Sales Record file"
        )
        assert out.limit == 10
        assert plan.limit == 100


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
            "Country": ColumnMetadata(
                column_name="Country",
                logical_type="string", sqlite_type="TEXT", nullable=True,
                original_name="Country", safe_name="col_country",
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

    def test_reject_groupby_sum_missing_target(self):
        plan = AnalyticsPlan(
            document_id="doc1",
            operation="groupby_sum",
            group_by="Country",
        )
        with pytest.raises(AnalyticsPlanValidationError):
            validate_plan(plan, self._col_meta())

    def test_reject_groupby_sum_missing_group_by(self):
        plan = AnalyticsPlan(
            document_id="doc1",
            operation="groupby_sum",
            target_column="Amount",
        )
        with pytest.raises(AnalyticsPlanValidationError):
            validate_plan(plan, self._col_meta())

    def test_reject_groupby_sum_non_numeric_target(self):
        plan = AnalyticsPlan(
            document_id="doc1",
            operation="groupby_sum",
            target_column="Country",
            group_by="Country",
        )
        with pytest.raises(AnalyticsPlanValidationError):
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
            col_types[h] = infer_logical_type(sample_df[h])

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
                values.append(normalize_cell(row[h], col_types[h]))
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

    def test_groupby_sum_returns_highest_revenue_country_not_highest_count(self, in_memory_db):
        df = pd.DataFrame(
            [
                {"Country": "The Gambia", "Total Revenue": 100.0},
                {"Country": "The Gambia", "Total Revenue": 100.0},
                {"Country": "The Gambia", "Total Revenue": 100.0},
                {"Country": "The Gambia", "Total Revenue": 100.0},
                {"Country": "USA", "Total Revenue": 1200.0},
                {"Country": "USA", "Total Revenue": 800.0},
            ]
        )
        doc_id, table_name, col_meta = self._ingest_sample(in_memory_db, df)
        in_memory_db.row_factory = sqlite3.Row

        plan = AnalyticsPlan(
            document_id=doc_id,
            operation="groupby_sum",
            target_column="Total Revenue",
            group_by="Country",
            order="value_desc",
            top_n=1,
        )
        compiled = compile_plan(plan, table_name=table_name, column_metadata=col_meta)
        rows = in_memory_db.execute(compiled.sql, tuple(compiled.parameters)).fetchall()

        assert len(rows) == 1
        assert rows[0]["key"] == "USA"
        assert rows[0]["value"] == pytest.approx(2000.0)


# ============================================================================
# Markdown display for chat (deterministic analytics)
# ============================================================================


def test_format_select_rows_markdown_dates_and_integers():
    from backend.analytics.display_markdown import format_analytics_result_markdown
    from backend.analytics.models import AnalyticsResult

    meta = {
        "Order Date": ColumnMetadata(
            column_name="Order Date",
            logical_type="date",
            sqlite_type="INTEGER",
            nullable=False,
            original_name="Order Date",
            safe_name="order_date",
        ),
        "Units Sold": ColumnMetadata(
            column_name="Units Sold",
            logical_type="integer",
            sqlite_type="INTEGER",
            nullable=False,
            original_name="Units Sold",
            safe_name="units_sold",
        ),
    }
    ar = AnalyticsResult(
        summary="Retrieved 1 matching row(s).",
        sql="SELECT 1",
        parameters=[],
        data={
            "rows": [{"Order Date": 1275004800, "Units Sold": 9925}],
            "row_count": 1,
        },
        document_id="d1",
        sheet_name="Sheet1",
    )
    md = format_analytics_result_markdown(ar, meta)
    assert "2010-05-28" in md
    assert "9,925" in md
    assert "| Order Date |" in md
    assert "| Units Sold |" in md


def test_format_scalar_analytics_summary_only():
    from backend.analytics.display_markdown import format_analytics_result_markdown
    from backend.analytics.models import AnalyticsResult

    ar = AnalyticsResult(
        summary="Counted 100 rows.",
        sql="SELECT COUNT",
        parameters=[],
        data={"count": 100},
        document_id="d1",
        sheet_name="Sheet1",
    )
    md = format_analytics_result_markdown(ar, {})
    assert md == "Counted 100 rows."


def test_format_groupby_count_table():
    from backend.analytics.display_markdown import format_analytics_result_markdown
    from backend.analytics.models import AnalyticsResult

    ar = AnalyticsResult(
        summary="Computed group-by counts for 'Region' (top 50).",
        sql="SELECT 1",
        parameters=[],
        data={"rows": [{"key": "A", "count": 3}, {"key": "B", "count": 1}]},
        document_id="d1",
        sheet_name="Sheet1",
    )
    md = format_analytics_result_markdown(ar, {})
    assert "| key | count |" in md
    assert "| A | 3 |" in md


def test_format_groupby_sum_value_no_scientific_notation():
    from backend.analytics.display_markdown import format_analytics_result_markdown
    from backend.analytics.models import AnalyticsResult

    ar = AnalyticsResult(
        summary="Computed group-by sums of 'Profit' by 'SalesChannel' (top 1).",
        sql="SELECT 1",
        parameters=[],
        data={"rows": [{"key": "Online", "value": 34189.2}]},
        document_id="d1",
        sheet_name="Sheet1",
    )
    md = format_analytics_result_markdown(ar, {})
    assert "| Online | 34,189.2 |" in md
    assert "e+" not in md.lower()


def test_format_select_rows_coerces_sqlite_row_objects():
    """sqlite3.Row is not isinstance(..., dict); formatter must still emit a table."""
    import sqlite3

    from backend.analytics.display_markdown import format_analytics_result_markdown
    from backend.analytics.models import AnalyticsResult

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE t (Region TEXT, Units INTEGER)")
    conn.execute("INSERT INTO t VALUES ('Australia', 9925)")
    row = conn.execute("SELECT * FROM t").fetchone()
    ar = AnalyticsResult(
        summary="Retrieved 1 matching row(s).",
        sql="SELECT 1",
        parameters=[],
        data={"rows": [row], "row_count": 1},
        document_id="d1",
        sheet_name="Sheet1",
    )
    md = format_analytics_result_markdown(ar, {})
    assert "| Region |" in md
    assert "| Australia |" in md
    assert "9,925" in md
    assert "**Data:**" not in md


# ============================================================================
# Filter normalizer + router + time grain + groupby_ratio
# ============================================================================


def test_normalize_filter_maps_user_phrase_to_profile_value():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE sales (category TEXT, country TEXT, revenue REAL)")
    conn.executemany(
        "INSERT INTO sales VALUES (?,?,?)",
        [("Fruits", "ES", 10.0), ("Fruits", "US", 20.0), ("Veg", "US", 5.0)],
    )
    col_meta = {
        "ProductCategory": ColumnMetadata(
            "ProductCategory", "string", "TEXT", True, "ProductCategory", "category"
        ),
        "Country": ColumnMetadata("Country", "string", "TEXT", True, "Country", "country"),
        "Revenue": ColumnMetadata("Revenue", "float", "REAL", True, "Revenue", "revenue"),
    }
    profile = DatasetProfile(
        row_count=3,
        columns={
            "ProductCategory": ColumnProfile(
                logical_type="string",
                null_ratio=0.0,
                distinct_count=2,
                top_values={"Fruits": 2, "Veg": 1},
            ),
        },
    )
    plan = AnalyticsPlan(
        document_id="d1",
        operation="groupby_sum",
        target_column="Revenue",
        group_by="Country",
        filters=[
            AnalyticsFilter(column="ProductCategory", operator="eq", value="fruits"),
        ],
    )
    out = normalize_analytics_plan_filters(
        plan, col_meta, profile, conn, "sales"
    )
    assert out.filters[0].value == "Fruits"
    validate_plan(out, col_meta)
    compiled = compile_plan(out, table_name="sales", column_metadata=col_meta)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(compiled.sql, tuple(compiled.parameters)).fetchall()
    assert len(rows) == 2
    keys = {r["key"] for r in rows}
    assert keys == {"ES", "US"}


def test_normalize_year_equals_on_integer_year_becomes_eq_and_sum_runs():
    """LLMs often emit year_equals on a spreadsheet Year column; only date columns allow it."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE sales (c_year INTEGER, c_rev REAL)")
    conn.executemany(
        "INSERT INTO sales VALUES (?,?)",
        [(2024, 10.0), (2025, 100.0), (2025, 50.0)],
    )
    col_meta = {
        "Year": ColumnMetadata("Year", "integer", "INTEGER", True, "Year", "c_year"),
        "Revenue": ColumnMetadata("Revenue", "float", "REAL", True, "Revenue", "c_rev"),
    }
    plan = AnalyticsPlan(
        document_id="d1",
        operation="sum",
        target_column="Revenue",
        filters=[AnalyticsFilter(column="Year", operator="year_equals", value=2025)],
    )
    with pytest.raises(AnalyticsPlanValidationError):
        validate_plan(plan, col_meta)

    out = normalize_analytics_plan_filters(plan, col_meta, None, None, None)
    assert out.filters[0].operator == "eq"
    assert out.filters[0].value == 2025
    validate_plan(out, col_meta)
    compiled = compile_plan(out, table_name="sales", column_metadata=col_meta)
    conn.row_factory = sqlite3.Row
    row = conn.execute(compiled.sql, tuple(compiled.parameters)).fetchone()
    assert row["sum_value"] == pytest.approx(150.0)


def test_normalize_year_equals_on_date_column_not_rewritten():
    col_meta = {
        "OrderDate": ColumnMetadata(
            "OrderDate", "date", "INTEGER", True, "OrderDate", "c_od"
        ),
        "Revenue": ColumnMetadata("Revenue", "float", "REAL", True, "Revenue", "c_rev"),
    }
    plan = AnalyticsPlan(
        document_id="d1",
        operation="sum",
        target_column="Revenue",
        filters=[AnalyticsFilter(column="OrderDate", operator="year_equals", value=2025)],
    )
    out = normalize_analytics_plan_filters(plan, col_meta, None, None, None)
    assert out is plan or out.filters[0].operator == "year_equals"
    assert out.filters[0].operator == "year_equals"
    validate_plan(out, col_meta)


class TestAnalyticsRouterExpanded:
    def test_monthly_sales_trend_routes(self):
        r = AnalyticsRouter()
        d = r.decide("Monthly sales trend by category")
        assert d.use_analytics is True

    def test_top_profitable_products_routes(self):
        r = AnalyticsRouter()
        d = r.decide("Top 5 most profitable products")
        assert d.use_analytics is True

    def test_online_offline_comparison_routes(self):
        r = AnalyticsRouter()
        d = r.decide("Online vs Offline revenue comparison")
        assert d.use_analytics is True

    def test_generic_most_without_metric_does_not_route_bi_branch(self):
        r = AnalyticsRouter()
        d = r.decide("What is the most popular programming language?")
        assert d.use_analytics is False

    def test_who_has_most_profit_routes(self):
        r = AnalyticsRouter()
        d = r.decide("Who has the most profit?")
        assert d.use_analytics is True


def test_repair_select_rows_to_groupby_superlative_salesperson_profit():
    plan = AnalyticsPlan(
        document_id="d1",
        operation="select_rows",
        limit=50,
    )
    column_names = ["Sales Person", "Profit", "Qty"]
    column_types = {
        "Sales Person": "string",
        "Profit": "float",
        "Qty": "integer",
    }
    q = "which sales person has the most profit overall in sales?"
    out = _repair_select_rows_to_groupby_superlative(
        plan, q, column_names, column_types
    )
    assert out.operation == "groupby_sum"
    assert out.target_column == "Profit"
    assert out.group_by == "Sales Person"
    assert out.order == "value_desc"
    assert out.top_n == 1


def test_repair_select_rows_unchanged_without_which_who_superlative():
    plan = AnalyticsPlan(document_id="d1", operation="select_rows", limit=10)
    column_names = ["Sales Person", "Profit"]
    column_types = {"Sales Person": "string", "Profit": "float"}
    out = _repair_select_rows_to_groupby_superlative(
        plan, "show me the first 10 rows", column_names, column_types
    )
    assert out.operation == "select_rows"


def test_groupby_ratio_sql():
    col_meta = {
        "Cat": ColumnMetadata("Cat", "string", "TEXT", True, "Cat", "c_cat"),
        "Profit": ColumnMetadata("Profit", "float", "REAL", True, "Profit", "c_profit"),
        "Revenue": ColumnMetadata("Revenue", "float", "REAL", True, "Revenue", "c_rev"),
    }
    plan = AnalyticsPlan(
        document_id="d1",
        operation="groupby_ratio",
        target_column="Profit",
        denominator_column="Revenue",
        group_by="Cat",
        top_n=10,
    )
    validate_plan(plan, col_meta)
    compiled = compile_plan(plan, table_name="t", column_metadata=col_meta)
    assert "SUM(c_profit)" in compiled.sql
    assert "NULLIF(SUM(c_rev)" in compiled.sql


def test_groupby_sum_monthly_time_grain():
    col_meta = {
        "OrderDate": ColumnMetadata(
            "OrderDate", "date", "INTEGER", True, "OrderDate", "c_od"
        ),
        "Cat": ColumnMetadata("Cat", "string", "TEXT", True, "Cat", "c_cat"),
        "Amount": ColumnMetadata("Amount", "float", "REAL", True, "Amount", "c_amt"),
    }
    t_jan = int(datetime(2024, 1, 10, tzinfo=timezone.utc).timestamp())
    t_feb = int(datetime(2024, 2, 5, tzinfo=timezone.utc).timestamp())
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (c_od INTEGER, c_cat TEXT, c_amt REAL)")
    conn.executemany(
        "INSERT INTO t VALUES (?,?,?)",
        [(t_jan, "A", 100.0), (t_jan, "B", 50.0), (t_feb, "A", 30.0)],
    )
    plan = AnalyticsPlan(
        document_id="d1",
        operation="groupby_sum",
        target_column="Amount",
        group_by="Cat",
        time_column="OrderDate",
        time_grain="month",
    )
    validate_plan(plan, col_meta)
    compiled = compile_plan(plan, table_name="t", column_metadata=col_meta)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(compiled.sql, tuple(compiled.parameters)).fetchall()
    by_period = {(r["time_bucket"], r["key"]): r["value"] for r in rows}
    assert by_period[("2024-01", "A")] == pytest.approx(100.0)
    assert by_period[("2024-01", "B")] == pytest.approx(50.0)
    assert by_period[("2024-02", "A")] == pytest.approx(30.0)


def test_repair_buys_most_from_rowcount_to_quantity_sum():
    """'Buys the most' must not use groupby_count (row frequency)."""
    column_types = {
        "Country": "string",
        "ProductCategory": "string",
        "Quantity": "integer",
        "Revenue": "float",
    }
    column_names = list(column_types.keys())
    plan = AnalyticsPlan(
        document_id="d1",
        operation="groupby_count",
        group_by="Country",
        filters=[
            AnalyticsFilter(column="ProductCategory", operator="eq", value="Fruits"),
        ],
    )
    out = _repair_rowcount_plan_to_quantity_sum(
        plan,
        "Which country buys the most fruits?",
        column_names,
        column_types,
    )
    assert out.operation == "groupby_sum"
    assert out.target_column == "Quantity"
    assert out.group_by == "Country"
    assert out.order == "value_desc"
    assert len(out.filters) == 1


def test_repair_skips_how_many_orders():
    plan = AnalyticsPlan(
        document_id="d1",
        operation="groupby_count",
        group_by="Country",
    )
    column_types = {"Country": "string", "Quantity": "integer"}
    out = _repair_rowcount_plan_to_quantity_sum(
        plan,
        "How many orders per country?",
        list(column_types.keys()),
        column_types,
    )
    assert out.operation == "groupby_count"


def test_format_markdown_time_bucket_value_columns():
    from backend.analytics.display_markdown import format_analytics_result_markdown
    from backend.analytics.models import AnalyticsResult

    ar = AnalyticsResult(
        summary="Monthly totals.",
        sql="SELECT 1",
        parameters=[],
        data={
            "rows": [
                {"time_bucket": "2024-01", "value": 150.0},
                {"time_bucket": "2024-02", "value": 30.0},
            ]
        },
        document_id="d1",
        sheet_name="Sheet1",
    )
    md = format_analytics_result_markdown(ar, {})
    assert "| period | value |" in md
    assert "2024-01" in md


# ============================================================================
# strip_filename_from_query (analytics decomposer pre-processing)
# ============================================================================


from backend.services.chat.intents import strip_filename_from_query as _strip_filename


class TestStripFilenameFromQuery:
    def test_removes_in_filename_file(self):
        q = "What is the total revenue in 2025 in Advanced_Sales_Dataset file ?"
        out = _strip_filename(q)
        assert "Advanced_Sales_Dataset" not in out
        assert "total revenue" in out
        assert "2025" in out
        assert out.endswith("?")

    def test_removes_in_filename_spreadsheet(self):
        q = "What is the total revenue in 2025 in Advanced_Sales_Dataset spreadsheet?"
        out = _strip_filename(q)
        assert "Advanced_Sales_Dataset" not in out
        assert "2025" in out

    def test_removes_from_filename(self):
        q = "Show revenue from Sales-Data-2025 file"
        out = _strip_filename(q)
        assert "Sales-Data-2025" not in out
        assert "revenue" in out.lower()

    def test_removes_of_filename_dataset(self):
        q = "Total profit of my_report dataset"
        out = _strip_filename(q)
        assert "my_report" not in out
        assert "profit" in out.lower()

    def test_preserves_query_without_filename(self):
        q = "What is the total revenue in 2025?"
        out = _strip_filename(q)
        assert "total revenue" in out
        assert "2025" in out
        assert out.endswith("?")

    def test_preserves_in_keyword_for_year(self):
        q = "Total sales in 2024?"
        out = _strip_filename(q)
        assert "2024" in out
        assert "sales" in out.lower()

    def test_removes_filename_with_extension(self):
        q = "What is the average price in Sales_Data.xlsx file?"
        out = _strip_filename(q)
        assert "Sales_Data" not in out
        assert "average price" in out

    def test_removes_quoted_filename(self):
        q = "Total units in 'Advanced_Sales_Dataset' file?"
        out = _strip_filename(q)
        assert "Advanced_Sales_Dataset" not in out
        assert "units" in out.lower()
