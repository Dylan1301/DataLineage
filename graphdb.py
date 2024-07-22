from lineage import *
from neo4j import GraphDatabase

def init_driver(uri, username, password):
    driver = GraphDatabase.driver(uri, auth=(username, password))

    driver.verify_connectivity

    return driver



def db_create_node(tx, node:LineageNode):
    node_type= node.type
    cypher=f"""CREATE (node: {node_type} {{name: $name, file_name: $file_name, id: $id}})"""

    
    name=node.name
    file_name=node.file_name
    id=node.id
    
    return tx.run(cypher, node_type=node_type, name=name, file_name=file_name, id=id)

def db_match_query_column(tx, node:LineageNode):

    nodetype = node.type
    cypher= f"""
    MATCH (query: {nodetype} {{file_name: '$file_name', id: $id, name:'$name'}})
    MATCH (column {{file_name: '$file_name'}}) WHERE column.id IN [$id_list]
    CREATE (query) -[:Contain_Column] -> (column)
"""
    file_name = node.file_name
    id = node.id
    id_list = ",".join([str(col.id) for col in node.downstream])
    print(id_list)

    return tx.run(cypher, file_name=file_name, id=id, id_list=id_list)

def db_match_alias():
    pass

def delete_node():
    pass


def db_list_node(tx, rootnode:LineageNode):
    queue=[]
    def walk(rootnode:LineageNode, queue):
        queue.append(rootnode)
        for node in rootnode.downstream:
            queue.append(node)
            for column in node.downstream:
                queue.append(column)
        
        for query in rootnode.downstream_related:
            walk(query, queue)
        
        return queue
        
    walk(rootnode)

    for node in queue:
        db_create_node(tx, node)
    for node in queue:
        if node.type =='alias' or node.type=='column':
            for column in node.downstream:
                db_create_rela(tx, node, column, 'Alias_Column')
            for query in node.upstream_related:
                db_create_rela(tx, node, query, 'Upstream_Related')
            for column2 in node.downstream_related:
                db_create_rela(tx, node, column2, 'Result_From')

        if node.type == 'query' or node.type== 'table':
            for column in node.downstream:
                db_create_rela(tx, node, column, 'Contain_Column')
            for query in node.downstream_related:
                db_create_rela(tx, node, query, 'Result_From_Query')
            for query in node.upstream_related:
                db_create_rela(tx, node, query, 'Result_To_Query')

def db_create_rela(tx, node:LineageNode, node2:LineageNode, rela_type):
    nodetype=node.type
    nodetype2=node2.type
    id= node.id
    file_name=node.file_name
    id2= node2.id
    file_name2= node2.file_name
    name= node.name
    name2=node.name

    cypher = f"""
    MATCH (node: {nodetype} {{file_name: '$file_name', id: $id, name:'$name'}})
    MATCH (node2: {nodetype2} {{file_name: '$file_name2', id: $id2, name:'$name2'}})
    CREATE (node) -[:{rela_type}] -> (node2)
    """

    return tx.run(cypher,file_name=file_name, file_name2=file_name2, id=id, id2=id2, name=name, name2=name2)



# with driver.session() as session:
#     nodes = session.execute_write(db_create_node,name='NodeA', relationship='Contain')
