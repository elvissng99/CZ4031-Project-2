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
    order by
      revenue desc,
      o_orderdate;
'''

q2 = ''' 
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
      c_custkey = o_custkey
      and l_orderkey = o_orderkey
    group by
      l_orderkey,
      o_orderdate,
      o_shippriority
    order by
      revenue desc,
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

query_diff = query_difference(q1_parsed, q2_parsed)

#find diff in qep
seq_diff_length,seq_diff = get_path_difference(q1_root, q2_root)
print(seq_diff)


diag1 = graphviz.Digraph(graph_attr={'dpi':'50'})
diag2 = graphviz.Digraph(graph_attr={'dpi':'50'})
QEP_dfs(q1_root, "query1", diag1)
print("boundary")
QEP_dfs(q2_root, "query2", diag2)


qep_diff = difference_QEP(seq_diff)
pprint(qep_diff)
qep_diff_nl = qep_diff_to_natural(qep_diff)
for diff in qep_diff_nl:
  print(diff)
  print()



#uncomment to see GUI interface
# root = Tk()
# window = Window(root)
# root.mainloop()
