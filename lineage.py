from sqlglot import parse_one, exp, maybe_parse
from sqlglot.optimizer import Scope, build_scope, find_all_in_scope, qualify
from dataclasses import dataclass
import typing as t
from sqlglot.errors import SqlglotError


@dataclass
class LineageNode:
    name:str
    type: str
    scope: Scope = None
    related_query: exp.Expression = None
    downstream=  []
    downstream_related= [] # For query node related to the current query node
    upstream_related= [] # For query node related to the current query node
    


def build_column_node(column
                      , scope, related_query):
    """
    Build node based on input, if input column object has alias/function wrapped -> sepearated into 2 node 

    input: exp.Column/ exp.Alias component
    output: LineageNode object of the column
    
    """
    if column.alias == '' and len(list(column.find_all(exp.Column))) == 1:
        # no alias found
        node = LineageNode(column.alias_or_name, type = 'column', scope=scope, related_query=column)
    
    else:
        name = 'function' if column.alias == '' else column.alias
        node = LineageNode(name, type = 'alias', scope=scope, related_query=column)
        for col in column.find_all(exp.Column):
            node_col = build_column_node(col, scope=scope, related_query=col)
            node_col.upstream_related.append(node)
            node.downstream.append(node_col)

    return node


def nodelize(scope, name = None, upstream_related=None):

    if not name:
        name = 'temp'
    
    thisnode = LineageNode(name, type='query', scope=scope, related_query=scope.expression)
    if not upstream_related:
        thisnode.upstream_related.append(upstream_related)
    select_scope = scope.expression.selects
    for column in select_scope:
        column_node = build_column_node(column=column, scope=scope, related_query=scope.expression)
        thisnode.downstream.append(column_node)

    return thisnode

def build_lineage(scope, queue = []):
    
    rootnode = nodelize(scope, 'Root_Query')

    current_node=rootnode
    while len(queue) > 0:

        if scope.union_scopes:
            for union in scope.union_scopes:
                node = nodelize(union, name='union', upstream_related=rootnode)    
                current_node.downstream_related.append(node)
                queue.append(node)

        # For subquery, ctes and tables
        for key, source in scope.sources.items():
            if isinstance(source, exp.Table):
                node=LineageNode(key, type='table', scope=None, related_query=source)
                node.upstream_related.append(current_node)
            elif isinstance(source, Scope):
                node=nodelize(source, name=key, upstream_related=current_node)
                queue.append(node)
            current_node.downstream_related.append(node)

        match_column(current_node)

        current_node=queue.pop(0)
        

    return rootnode
        
        
def match_column(node: LineageNode):
    def match(node, column):

        if column.type == 'column':
            for find_column in node.downstream:
                if find_column.name == column.name:
                    column.downstream_related.append(find_column)
                    find_column.upstream_related.append(column)
                    return True

        elif column.type == 'alias' or column.type == 'function':
            for col in column.downstream:
                match(node, col)

    
    for col in node.downstream:
        match(node, col)
    


def get_lineage(sql, dialect = None):
    expression = maybe_parse(sql,  dialect=dialect)
    qualified = qualify.qualify(expression, dialect=dialect)

    scope = build_scope(qualified)

    if not scope:
        raise SqlglotError("Cannot build lineage, sql must be SELECT")
    
    
    lineage = build_lineage(scope)

    return lineage