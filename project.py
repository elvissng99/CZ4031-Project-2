from interface import *
from explain import *
from pprint import pprint
q1 = '''
set max_parallel_workers_per_gather =0;
EXPLAIN (VERBOSE, ANALYZE, FORMAT JSON)
select c_name, c_nationkey, c_mktsegment, c_acctbal, o_totalprice, n_name
from customer, orders, nation
where c_acctbal > 1000
and o_totalprice >100000
and customer.c_custkey = orders.o_orderkey
and customer.c_nationkey = nation.n_nationkey
group by c_name, c_nationkey, c_mktsegment, c_acctbal, o_totalprice, n_name;
'''

q2 = ''' 
set max_parallel_workers_per_gather =0;
EXPLAIN (VERBOSE, ANALYZE, FORMAT JSON)
select c_name, c_nationkey, c_mktsegment, c_acctbal, o_totalprice, n_name, l_extendedprice
from customer, orders, nation, lineitem
where c_acctbal > 1000
and o_totalprice >100000
and customer.c_custkey = orders.o_orderkey
and customer.c_nationkey = nation.n_nationkey
and orders.o_orderkey = lineitem.l_orderkey
group by c_name, c_nationkey, c_mktsegment, c_acctbal, o_totalprice, n_name, l_extendedprice;
'''


connection = connect_db()
q1_result  = execute_json(connection, q1)
q2_result  = execute_json(connection, q2)
# pprint(q1_result)
q1_root = buildQEP(q1_result)
q2_root = buildQEP(q2_result)


#parse sql
q1_parsed = parseSQL(q1)
q2_parsed = parseSQL(q2)
#find sql query differences
query_diff = query_difference(q1_parsed, q2_parsed)
query_diff_natural_language = sql_diff_to_natural_language(query_diff)

#algo to find the sequential changes to convert QEP1 into QEP2
qep_diff_length,qep_diff = get_path_difference(q1_root, q2_root)
pprint(qep_diff)

#output qep into image files
diag1 = graphviz.Digraph(graph_attr={'dpi':'50'})
diag2 = graphviz.Digraph(graph_attr={'dpi':'50'})
QEP_dfs(q1_root, "query1", diag1)
QEP_dfs(q2_root, "query2", diag2)


#natural language output strings
for diff in query_diff_natural_language:
  print(diff)
  print()

qep_diff_natural_language = diff_to_natural_language(qep_diff,query_diff)
for diff in qep_diff_natural_language:
  print(diff)
  print()

  



#uncomment to see GUI interface
root = Tk()
window = Window(root)
root.mainloop()
