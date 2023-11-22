from sqlglot import parse_one, exp
from sqlglot.optimizer.scope import *
from dataclasses import dataclass
import typing as t


@dataclass
class ColumnLineage:
    name:str
    alias: str
    source_table: exp.Expression
    related_query: exp.Expression
    downstream: t.List[ColumnLineage]


def get_select_cols(sql, dialect):
    expression = parse_one(sql, dialect=dialect)

    scope = build_scope(expression)

    column 