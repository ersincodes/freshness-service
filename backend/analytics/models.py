from __future__ import annotations

from datetime import date
from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field


class TableRef(BaseModel):
    """Reference to a particular ingested sheet."""

    document_id: str
    sheet_name: Optional[str] = None


class WherePredicate(BaseModel):
    """Restricted WHERE predicate."""

    column: str
    op: Literal[
        "=",
        "!=",
        "<",
        "<=",
        ">",
        ">=",
        "contains",
        "startswith",
        "endswith",
        "is_null",
        "is_not_null",
    ]
    value: Optional[Union[str, int, float, bool, date]] = None


class CountRowsPlan(BaseModel):
    type: Literal["count_rows"] = "count_rows"
    table: TableRef
    where: list[WherePredicate] = Field(default_factory=list)


class CountDistinctPlan(BaseModel):
    type: Literal["count_distinct"] = "count_distinct"
    table: TableRef
    column: str
    where: list[WherePredicate] = Field(default_factory=list)


class GroupByCountPlan(BaseModel):
    type: Literal["groupby_count"] = "groupby_count"
    table: TableRef
    group_by: str
    where: list[WherePredicate] = Field(default_factory=list)
    order: Literal["count_desc", "count_asc", "key_asc", "key_desc"] = "count_desc"
    top_n: int = 50


AnalyticsPlan = Annotated[
    Union[CountRowsPlan, CountDistinctPlan, GroupByCountPlan],
    Field(discriminator="type"),
]


class AnalyticsResult(BaseModel):
    """Normalized analytics result returned to ChatService."""

    summary: str
    sql: str
    parameters: list[object]
    data: dict
