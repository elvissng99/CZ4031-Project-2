from interface import *
from explain import *
from pprint import pprint
q1 = '''
set max_parallel_workers_per_gather =0;
EXPLAIN (VERBOSE, ANALYZE, FORMAT JSON)
select * from customer;
'''

q2 = ''' 
set max_parallel_workers_per_gather =0;
EXPLAIN (VERBOSE, ANALYZE, FORMAT JSON)
select c_name, c_mktsegment, c_acctbal, o_totalprice
from customer, orders
where c_acctbal > 1000
and o_totalprice >100000
and customer.c_custkey = orders.o_orderkey
group by c_name,c_mktsegment, c_acctbal, o_totalprice;
'''


connection = connect_db()
q1_result  = execute_json(connection, q1)
q2_result  = execute_json(connection, q2)
q2_result  = execute_json(connection, q2)
# pprint(q1_result)
q1_root = buildQEP(q1_result)
q2_root = buildQEP(q2_result)
QEP_bfs(q1_root)
print("boundary")
QEP_bfs(q2_root)
# QEP_bfs(q2_root)
results = get_path_difference(q1_root, q2_root)
print(results)


#uncomment to see GUI interface
# root = Tk()
# window = Window(root)
# root.mainloop()