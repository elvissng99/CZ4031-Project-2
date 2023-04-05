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

QEP_bfs(q1_root)
print()
QEP_bfs(q2_root)
print()
result = printSQL(q1)
pprint(result)

#uncomment to see GUI interface
# root = Tk()
# window = Window(root)
# root.mainloop()