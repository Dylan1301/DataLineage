from lineage import *
from neo4j import GraphDatabase

def init_driver(uri, username, password):
    driver = GraphDatabase.driver(uri, auth=(username,  password))

    driver.verify_connectivity

    return driver



def db_create_node(tx, node:LineageNode):
    cypher="""
    CREATE (node:$node_type {name:'$name', file_name:'$file_name', id:$id})
"""
    name=node.name
    file_name=node.file_name
    id=node.id

    return tx.run(cypher, name=name, file_name=file_name, id=id)

def db_match_query_column(tx, node:LineageNode):
    cypher= """
    MATCH (query: $nodetype {file_name: '$file_name', id: $id, name:'$name'})
    MATCH (column: 'Column' {file_name: '$file_name'}) WHERE column.id IN [$id_list]
    CREATE (query) -[:Contain_Column] -> (column)
"""
    nodetype = node.type
    file_name = node.file_name
    id = node.id
    id_list = ",".join([col.id for col in node.downstream])


    return tx.run(cypher, file_name=file_name, nodetype=nodetype, id=id, id_list=id_list)

def find_node():
    pass

def delete_node():
    pass


driver = init_driver('','','')
with driver.session() as session:
    nodes = session.execute_write(db_create_node,name='NodeA', relationship='Contain')
