from sqlglot import parse_one, exp, maybe_parse
from node import Node, Column, CTE, Table, Query, Alias, Function, Other


def build_node_column(column: exp.Expression, scope=None, file_name: str = None, source=""):
    """
    Description: Build expression column into Node representation. The node might be wrapped in multiple layer by Alias or functions
    Input
    """

    # Incase current instance is Column => Return Imediately
    if isinstance(column, exp.Column):
        return Column(scope=scope, name=column.name, file_name=file_name)

    if isinstance(column, exp.Star):
        return Other(scope=scope, name="Star", file_name=file_name)

    if isinstance(column, exp.Alias):
        node = Alias(scope=scope, name=column.alias, file_name=file_name)

    elif isinstance(column, exp.Condition):
        node = Function(scope=scope, name="Function", file_name=file_name)
    else:
        node = Other(scope=scope, name="Unknown", file_name=file_name)

    for child in column.find_all(Column):
        child_node = build_node_column(child, scope=scope, file_name=file_name)
        node.downstream.append(child_node)
        child_node.upstream.append(node)

    # Return alias or function node
    return node


def build_node(scope: Scope,
               scope_name: str | None = None) -> Node:
    
