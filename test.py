from lineage import *

sql = """
                              


select cola, colb from table
Union
select col1, col2 from table2


"""


# print(repr(get_lineage(sql)))
rootnode = get_lineage(sql)
print(rootnode.downstream_related)


