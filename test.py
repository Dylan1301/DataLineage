from sqlglot import parse_one, exp

object = parse_one("""
                   WITH CTE 
                   AS (SELECT * FROM Table 
                   )
                   
                   SELECT cola, colb + 1 AS c FROM (SELECT a, b FROM d) tmp
                   
                   INNER JOIN CTE c
                   ON tmp.cola=c.colb
                   
                    where tmp.cola <> 'A'
                   
                   
                   """)

print(repr(object))


# from sqlglot import parse_one
# import sqlglot
# import sqlglot.expressions as exp

# sql = """
# SELECT
#   rc.dateCooked,
#   r.name,
#   i.ingredient
# FROM recipeCooked rc
# INNER JOIN recipe r ON r.recipeID = rc.recipeID
# LEFT OUTER JOIN recipeIngredient ri ON ri.recipeID = r.recipeID
# LEFT OUTER JOIN ingredient i ON i.ingredientID = ri.ingredientID;
# """

# # node = sqlglot.parse_one(sql)

# # for join in node.args["joins"]:
# #     table = join.find(exp.Table).text("this")
# #     print(table)
# #     print(join.args["on"])
# #     print(join.args["kind"])

# for column in parse_one("SELECT a, b + 1 AS c FROM d").find_all(exp.Column):
#     print(column.alias_or_name)