from interface import *
from explain import *
from pprint import pprint
q1 = '''
set max_parallel_workers_per_gather =0;
EXPLAIN (VERBOSE, ANALYZE, FORMAT JSON)
SELECT
    o.o_orderpriority,
    SUM(l.l_quantity * l.l_extendedprice) AS revenue
FROM
    orders o
    JOIN lineitem l ON o.o_orderkey = l.l_orderkey
WHERE
    o.o_orderdate >= '1994-01-01'
    AND o.o_orderdate < '1995-01-01'
GROUP BY
    o.o_orderpriority
HAVING
    SUM(l.l_quantity * l.l_extendedprice) > (
        SELECT
            SUM(sub_l.l_quantity * sub_l.l_extendedprice) AS sub_revenue
        FROM
            lineitem sub_l
            JOIN orders sub_o ON sub_l.l_orderkey = sub_o.o_orderkey
        WHERE
            sub_o.o_orderdate >= '1994-01-01'
            AND sub_o.o_orderdate < '1995-01-01'
    )
ORDER BY
    revenue DESC,
    o.o_orderpriority;
'''

q2 = ''' 
set max_parallel_workers_per_gather =0;
EXPLAIN (VERBOSE, ANALYZE, FORMAT JSON)
SELECT
    o.o_orderpriority,
    SUM(l.l_quantity * l.l_extendedprice) AS revenue
FROM
    orders o
    JOIN lineitem l ON o.o_orderkey = l.l_orderkey
WHERE
    o.o_orderdate >= '1994-01-01'
    AND o.o_orderdate < '1995-01-01'
GROUP BY
    o.o_orderpriority
HAVING
    SUM(l.l_quantity * l.l_extendedprice) > 1000
ORDER BY
    revenue DESC,
    o.o_orderpriority;
'''
q1 = q1.lower()
q2 = q2.lower()

connection = connect_db()
q1_result  = execute_json(connection, q1)
q2_result  = execute_json(connection, q2)
# pprint(q1_result)
q1_root = buildQEP(q1_result)
q2_root = buildQEP(q2_result)


#parse sql
q1_parsed = parseSQL(q1)
q2_parsed = parseSQL(q2)
pprint(q1_parsed)
print()
pprint(q2_parsed)
#find sql query differences
query_diff = query_difference(q1_parsed, q2_parsed)
pprint(query_diff)
query_diff_natural_language = sql_diff_to_natural_language(query_diff)

#algo to find the sequential changes to convert QEP1 into QEP2
qep_diff_length,qep_diff = get_path_difference(q1_root, q2_root)
pprint(qep_diff)

#output qep into image files
QEP_dfs(q1_root, "query1")
QEP_dfs(q2_root, "query2")


# natural language output strings
# SQL differences
sql_text = convert_to_text(query_diff_natural_language)
print(sql_text)

# QEP differences
qep_diff_natural_language = diff_to_natural_language(qep_diff, query_diff)
qep_text = convert_to_text(qep_diff_natural_language)
print(qep_text)

  



#uncomment to see GUI interface
# root = Tk()
# window = Window(root)
# root.mainloop()
