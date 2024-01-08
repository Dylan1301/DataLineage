from sqlglot import parse_one, exp
from sqlglot.optimizer.scope import *
from dataclasses import dataclass
import typing as t


@dataclass
class LineageNode:
    name:str
    type: str
    scope: Scope = None
    related_query: exp.Expression
    downstream: t.List[t.LineageNode] = None
    downstream_related: t.List[t.LineageNode] = None # For query node related to the current query node
    upstream_related: t.LineageNode = None # For query node related to the current query node
    


def build_column_node(column
                      , scope, related_query):
    """
    Build node based on input, if input column object has alias/function wrapped -> sepearated into 2 node 

    input: exp.Column/ exp.Alias component
    output: LineageNode object of the column
    
    """
    if column.alias == '' and len(column.find_all(exp.Column)) == 1:
        # no alias found
        node = LineageNode(column.alias_or_name, type = '', scope=scope, related_query=related_query, downstream=[])
    
    else:
        name = 'function' if column.alias == '' else column.alias
        node = LineageNode(name, type = 'alias', scope=scope, related_query=related_query, downstream=[])
        for col in column.find_all(exp.Column):
            node_col = build_column_node(col)
            node_col.upstream_related.append(node)
            node.downstream.append(node_col)

    return node


def nodelize(scope, name = None, upstream_related=None):

    if not name:
        name = 'temp'
    
    thisnode = LineageNode(name, type='query', scope=scope, related_query=scope.expression, downstream=[], downstream_related=[], upstream_related=upstream_related)
    
    select_scope = sql.expression.selects
    for column in select_scope:
        column_node = build_column_node(column=column, scope=scope, related_query=scope.expression)
        thisnode.downstream.append(column_node)

    return thisnode

def build_lineage(scope, current_processing = None, queue = [], node_queue = []):
    
    current_node = nodelize(scope, current_processing)

    queue.append((key, source, current_node) for key, source in scope.sources.items())

    while not node_queue:
        current_processing = current_node
        while not queue:
            item = queue.pop()

            if isinstance(item[1], exp.Table):
                child_node= LineageNode(name, type='table', scope=None, related_query=item[1], downstream=[], downstream_related=[], upstream_related=current_node)
            elif isinstance(item[1], Scope):
                child_node = nodelize(item[1], item[0], upstream_related=current_node)
            
            node_queue.append(child_node)

            current_node.downstream.append(child_node)
        
    


    

def find_column(column: exp.Column, scope: Scope, source= None, related_query = None, sources_table = None, upstream = None):
    """
    
    """

    
    if not sources_table:
        sources_table = scope.sources

    if column.table:
        source_table= {column.table: sources_table[column.table]}
    else:
        source_table = {k:v for k, v in sources_table.items if [x for x in v.expression.selects if x.alias_or_name == column]}

    node = LineageNode(name=column.name, source_table=source_table)


    
    node = LineageNode(column.name, source_table=source_table, related_query=sql, downstream=[], type = 'column')
    


        

