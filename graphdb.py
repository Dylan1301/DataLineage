from lineage import *
from neo4j import GraphDatabase

def init_driver(uri, username, password):
    driver = GraphDatabase.driver(uri, auth=(username,  password))

    driver.verify_connectivity

    return driver



def create_node(tx, name, relationship):
    pass

def find_node():
    pass

def delete_node():
    pass


driver = init_driver('','','')
with driver.session() as session:
    nodes = session.execute_write(create_node,name='NodeA', relationship='Contain')
