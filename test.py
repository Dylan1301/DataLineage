from lineage import *
from pprint import pprint
sql = """
                              


select cola, colb from table
Union
select col1, col2 from table2


"""


# print(repr(get_lineage(sql)))
rootnode = get_lineage(sql)
pprint(rootnode.downstream[0].downstream[0])


# print(rootnode.downstream[0])





