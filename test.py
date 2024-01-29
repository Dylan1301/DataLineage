from lineage import *
from graphdb import *
from pprint import pprint
sql = """
                              


select cola, colb from table
Union
select col1, col2 from table2


"""


# print(repr(get_lineage(sql)))
rootnode = get_lineage(sql, file_name='test1')
# pprint(rootnode)


# print(rootnode.downstream[0])


driver=init_driver(r"neo4j://localhost[:7687] ", "neo4j", "DataLineage")

driver

