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
    downstream_related: t.List[t.LineageNode] = None
    upstream_related: t.LineageNode = None
    


def build_column_node(column: exp.
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
            node.downstream.append(node_col)

    return node




def nodelize(scope, queue=[], name = None, upstream_related=None):
    if not name:
        name = 'temp'
    
    thisnode = LineageNode(name, type='query', scope=scope, related_query=scope.expression, downstream=[], downstream_related=[], upstream_related=upstream_related)
    
    select_scope = sql.expression.selects
    for column in select_scope:
        thisnode.downstream.append(build_column_node(column=column, scope=scope, related_query=scope.expression))


    if queue: 
        return upstream_related
    


    # chưa xử lí th alias






def list_alias(sql, sources, upstream= None):
    """
    break down all alias and column name of a single column in a sql statement
    """
    select_scope = sql.expression.selects
    aliases = [x for x in select_scope if x.alias]
    direct_columns = [x for x in select_scope if not x.alias]
    

    # Create alias node to connect with other components
    for alias in aliases:
        alias_node = LineageNode(alias.name, source_table= sources, related_query=None, related_node=[], downstream=[])
        upstream.downstream.append(alias_node)


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
    


        

