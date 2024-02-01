from lineage import *
from graphdb import *
from pprint import pprint
from neo4j import GraphDatabase
sql = """
                              


select cola, colb from table
Union
select col1, col2 from table2


"""


# print(repr(get_lineage(sql)))
rootnode = get_lineage(sql, file_name='test1')
# pprint(rootnode)


# print(rootnode.downstream[0])


# driver=init_driver("bolt://127.0.0.1:7687", "neo4j", "12345678")

address="bolt://localhost:7687"
auth=("neo4j", "12345678")
driver = GraphDatabase.driver(address, auth=auth)
driver.verify_authentication()


# tx = driver.session()

# db_create_node(tx, node=rootnode)

with driver.session() as session:
    
    session.execute_write(db_create_node,node=rootnode)
    for node in rootnode.downstream:
        session.execute_write(db_create_node, node=node)
    session.execute_write(db_match_query_column,node=rootnode)


