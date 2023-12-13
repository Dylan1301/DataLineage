from sqlglot import parse_one, exp
from sqlglot.optimizer.scope import *
from dataclasses import dataclass
import typing as t


@dataclass
class Node:
    name: str
    type: str
    scope: Scope = None
    downstream: t.List[Node] = []
    related: [] = None
    source: exp.Expression
    expression: exp.Expression
    others: {}



def search_column():
    pass

def column_to_node(column: exp.Column, sources: dict, scope, expression, upstream_node, related=None, others=None):
    if column.is_star:
        pass

    source_table = column.table

    if source_table:
        source = sources[source_table]
    
    source = ((select for select in v.expression.selects if  select.alias_or_name== column) 
              for k,v in sources.items() if isinstance(v.expression, ))
    


    search_column()

    return None


# class ColumnLineage:
#     name:str
#     alias: str
#     source_table: exp.Expression
#     related_query: exp.Expression
#     downstream: t.List[ColumnLineage]

