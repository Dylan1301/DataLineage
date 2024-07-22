from sqlglot import Scope, Expression
from typing import List, list, Self, Iterator
from enum import Enum

from dataclasses import dataclass, field
from itertools import count


@dataclass
class Node:
    id: int = field(default_factory=count().__next__)
    scope: Scope|None = None
    relate_query_string: str|None = None
    downstream_related: list[Self] = field(default_factory=list)
    upstream_related: list[Self] = field(default_factory=list)
    file_name: str|None = None
    datatype: str|None = None
    nodetype: str|None = None

    def walk(self) -> Iterator[Self]:
        yield self

        for node in self.downstream:
            yield from node.walk()


@dataclass
class Alias(Node):
    pass


@dataclass
class Column(Node):
    pass


@dataclass
class Table(Node):
    pass


@dataclass
class Query(Node):
    pass


@dataclass
class CTE(Node):
    pass


@dataclass
class Join(Node):
    pass


@dataclass
class Where(Node):
    pass


@dataclass
class Other(Node):
    pass
