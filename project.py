from interface import *
from explain import *
from pprint import pprint
q1 = '''
set max_parallel_workers_per_gather =0;
EXPLAIN (VERBOSE, ANALYZE, FORMAT JSON)
select
      l_orderkey,
      sum(l_extendedprice * (1 - l_discount)) as revenue,
      o_orderdate,
      o_shippriority
    from
      customer,
      orders,
      lineitem
    where
      c_mktsegment = 'BUILDING'
      and c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and o_totalprice > 10
      and l_extendedprice > 10
    group by
      l_orderkey,
      o_orderdate,
      o_shippriority
    having
      sum(l_extendedprice * (1 - l_discount)) > 1000
    order by
      revenue desc,
      o_orderdate;
'''

q2 = ''' 
set max_parallel_workers_per_gather =0;
EXPLAIN (VERBOSE, ANALYZE, FORMAT JSON)
select
      l_orderkey,
      o_orderdate,
      o_shippriority
    from
      customer,
      orders,
      lineitem
    where
      c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and o_totalprice > 100

    order by
      o_orderdate;
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
pprint(query_diff)

#algo to find the sequential changes to convert QEP1 into QEP2
qep_diff_length,qep_diff = get_path_difference(q1_root, q2_root)
pprint(qep_diff)

#output qep into image files
diag1 = graphviz.Digraph(graph_attr={'dpi':'50'})
diag2 = graphviz.Digraph(graph_attr={'dpi':'50'})
QEP_dfs(q1_root, "query1", diag1)
QEP_dfs(q2_root, "query2", diag2)


#natural language output strings
output = diff_to_natural_language(qep_diff,query_diff)
for diff in output:
  print(diff)
  print()



#uncomment to see GUI interface
# root = Tk()
# window = Window(root)
# root.mainloop()
