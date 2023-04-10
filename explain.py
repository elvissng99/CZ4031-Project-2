import psycopg2
import configparser
from collections import deque
import sqlparse
import graphviz

# from pprint import pprint
class Node:
    def __init__(self,information, parent=None):
        self.parent = parent
        self.index = None
        self.children = []
        self.node_type = information['Node Type']
        del information['Node Type']
        self.information = information
        if 'Plans' in information:
            plans = information['Plans']
            del information['Plans']
            for plan in plans:
                child = Node(plan, self)
                self.children.append(child)

def connect_db():
    config = configparser.ConfigParser()
    config.read('config.ini')
    default = config['DEFAULT']
    return psycopg2.connect(
        host=default['DB_HOST'],
        database=default['DB_NAME'],
        user=default['DB_USER'],
        password= default['DB_PASSWORD']
        )

def helper_function(connection, string_query):
    cursor = connection.cursor()
    cursor.execute(string_query)
    records = cursor.fetchall()
    print(records[0][0][0])

def execute_json(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor.fetchall()[0][0][0]['Plan']

def buildQEP(query_result_json):
    return Node(query_result_json)

def QEP_dfs(root):
    q = deque()
    cur = root
    q.append(cur)
    while(len(q) > 0):
        node = q.popleft()
        if node.parent is not None:
            d.edge(str(node.parent.index), str(node.index))
        for child in node.children:
            QEP_dfs(child, name, d)
        d.node(str(node.index), label = node.node_type)
    d.render(name, format="png")

def parseSQL(query):
    parsed = sqlparse.parse(query)[1]
    # print(parsed)
    return iterate_parsedSQL(parsed)
    
def iterate_parsedSQL(parsed):
    keyword = 'list'
    excluded_keywords = ["and","or","not","between","in","exists", "as"]
    result = {}
    result[keyword] = []
    temp = result[keyword]
    bracket_keyword = None
    for e in parsed.tokens:
        if(not e.is_whitespace and e.value != ","):    
            if(e.is_keyword):
                # print(1,e.value)
                if(e.value not in excluded_keywords):
                    keyword = e.value
                    result[keyword] = []
                    temp = result[keyword]
                else:
                    temp.append(e.value)
            elif (e.is_group):
                if isinstance(e,sqlparse.sql.Identifier) or isinstance(e,sqlparse.sql.IdentifierList):
                    if(keyword == 'order by'):
                        if isinstance(e,sqlparse.sql.Identifier):
                            temp.append(e.get_name() +" " + ("ASC" if e.get_ordering() == None else e.get_ordering()))
                            # print(e.get_name(), "ASC" if e.get_ordering() == None else e.get_ordering())
                        else:
                            for item in e.get_identifiers():
                                # print(item.get_name(), "ASC" if item.get_ordering() == None else item.get_ordering())
                                string = item.get_name() + " " +("ASC" if item.get_ordering() == None else item.get_ordering())
                                temp.append(string)
                    else:
                        sub_result = iterate_parsedSQL(e)
                        # print("sub for identifier/identifierlist",sub_result)
                        temp.extend(sub_result['list'])
                elif isinstance(e,sqlparse.sql.Comparison):
                    temp.append(e.value)
                    # print(2,e)
                elif isinstance(e,sqlparse.sql.Parenthesis) or isinstance(e,sqlparse.sql.Where):
                    sub_result = iterate_parsedSQL(e)
                    if(len(sub_result['list'])> 0):
                        temp.extend(sub_result['list'])
                    if('select' in sub_result):
                        temp.append(sub_result)
                    elif('where' in sub_result):
                        result['where'] = sub_result['where']
                else:
                    temp.append(e.value)
                    # print(3,e)
            else:
                if(e.value ==';'):
                    # print("END")
                    pass
                else:
                    
                    if(e.value == '('):
                        bracket_keyword = keyword
                        temp.append(e.value)
                    elif(e.value == ')'):
                        result[bracket_keyword].append(e.value)
                    else:
                        temp.append(e.value)
                    # print(4,e)
    if 'where' in result:
        if 'exists' in result['where']:
            reformat_WHERE_subquery(result,'exists')
        elif 'in' in result['where']:
            reformat_WHERE_subquery(result,'in')
    return result

def reformat_WHERE_subquery(result,key):
    repeat = True
    while(repeat):
        repeat = False
        for k,v in enumerate(result['where']):
            if v == key:
                if(result['where'][k-1] == 'not'):
                    key = 'not '+key
                    k -=1
                    result['where'].pop(k) 
                result['where'][k] = {key:result['where'][k+3]}
                result['where'].pop(k+1)
                result['where'].pop(k+1)
                result['where'].pop(k+1)
                repeat = True
                break

def query_difference(q1,q2):
    diff_result = {}
    q1_set = set(q1.keys())
    q2_set = set(q2.keys())
    common_keys = q1_set.intersection(q2_set)
    print("Common keys: ", common_keys)
    for key in common_keys:
        diff = set(q1[key]).symmetric_difference(set(q2[key]))
        # print("Diff: ", diff)
        if diff:
            diff_result[key] = list(diff)
    print("Difference in q2 compared to q1 is:\n", diff_result)
    return diff_result

# algo from here onwards
def initialize_index(node, index):
    """Initialize tree with post order numbering index. """
    if not node:
        return None

    # recursively initialize index
    for child in node.children:
        index = initialize_index(child, index)

    node.index = index
    index += 1
    return index

def initialize_node_list(node):
    node_list = []
    if not node:
        return None
    for child in reversed(node.children):
        node_list.extend(initialize_node_list(child))
    node_list.append(node)
    return node_list

def tree_edit_distance(tree1, tree2):
    """
    Parameters:
        tree1: Tree to transform
        tree2: Tree to transform into

    Recursion function to determine order of operations to transform tree1 into tree2:
    1. Delete node
    2. Insert node
    3. Update node
    4. Matched node

    The algorithm operates on sub-forests in each recursion.

    Return parameters:
        cost: Minimum tree edit distance
        path: Path taken for minimum tree edit distance
    """
    # if both trees empty, return
    if tree1 is None and tree2 is None:
        return 0, []

    # if tree1 is empty, insert all the nodes of tree2 into tree1
    if tree1 is None:
        path = [["Insert", str(tree2.node_type), str(tree2.index), tree2]]
        for child in tree2.children:
            _, child_path = tree_edit_distance(None, child)
            path.extend(child_path)
        return len(path), path

    # if tree2 is empty, delete all the nodes of tree1
    if tree2 is None:
        path = [["Delete", str(tree1.node_type), str(tree1.index), tree1]]
        for child in tree1.children:
            _, child_path = tree_edit_distance(child, None)
            path.extend(child_path)
        return len(path), path

    # sort children by node_type+index
    tree1.children.sort(key=lambda x: (x.node_type, x.index))
    tree2.children.sort(key=lambda x: (x.node_type, x.index))

    # if the nodes have same index and type, matching nodes
    if tree1.index == tree2.index and tree1.node_type == tree2.node_type:
        cost = 0
        path = [["Matched", str(tree1.node_type), str(tree1.index),"to",str(tree2.node_type),str(tree2.index),tree1,tree2]]
    # update nodes
    else:
        cost = 1
        path = [["Update" , str(tree1.node_type) , str(tree1.index) , "to" , str(tree2.node_type) , str(tree2.index),tree1,tree2]]

    min_cost = float('inf')
    min_path = []

    cost_perm = 0
    path_perm = []

    # calculate edit distance for each pair of corresponding children
    for child1, child2 in zip(tree1.children, tree2.children):
        child_cost, child_path = tree_edit_distance(child1, child2)
        cost_perm += child_cost
        path_perm.extend(child_path)

    # delete extra children in tree1
    for child1 in tree1.children[len(tree2.children):]:
        child_cost, child_path = tree_edit_distance(child1, None)
        cost_perm += child_cost
        path_perm.extend(child_path)

    # insert extra children in tree1
    for child2 in tree2.children[len(tree1.children):]:
        child_cost, child_path = tree_edit_distance(None, child2)
        cost_perm += child_cost
        path_perm.extend(child_path)

    min_cost = cost_perm
    min_path = path_perm

    # add the minimum cost and the corresponding path to the overall cost and path
    cost += min_cost
    path.extend(min_path)

    return cost, path


def get_path_difference(tree1, tree2):
    initialize_index(tree1, 0)
    initialize_index(tree2, 0)
    seq_diff = tree_edit_distance(tree1, tree2)
    return seq_diff

def difference_QEP(seq_diff):
    diffs = {
        'delete':[],
        'insert':[],
        'update':[]
    }
    for diff in seq_diff:
        if "Matched" not in diff:
            if "Update" in diff:
                diffs['update'].append([diff[6],diff[7]])
            elif "Delete" in diff:
                diffs['delete'].append(diff[3])
            elif "Insert" in diff:
                diffs['insert'].append(diff[3])
            else:
                print("SHOULD NOT HAPPEN")

    #update
    #check that, if update a to b, then update b to a, remove because its the same thing
    to_be_deleted = []
    for i in range(len(diffs['update'])):
        node_pair1 = diffs['update'][i]
        for j in range(i+1,len(diffs['update'])):
            node_pair2 = diffs['update'][j]
            if(node_pair1 != node_pair2 and compare_nodes(node_pair1[0], node_pair2[1]) and compare_nodes(node_pair1[1], node_pair2[0])):
                to_be_deleted.append(node_pair1)
                to_be_deleted.append(node_pair2)
                break
    
    if(len(to_be_deleted)>0):
        for node_pair in to_be_deleted:
            diffs['update'].remove(node_pair)
    to_be_deleted = []
    #check for delete and insert if delete a, then insert a, remvoe because its the same thing
    for node1 in diffs['delete']:
        for node2 in diffs['insert']:
            if(compare_nodes(node1, node2)):
                to_be_deleted.append(node1)
                to_be_deleted.append(node2)
    if(len(to_be_deleted)>0):
        for node in to_be_deleted:
            if(node in diffs['delete']):
                diffs['delete'].remove(node)
            elif(node in diffs['insert']):
                diffs['insert'].remove(node)
    return diffs

def compare_nodes(node1,node2):
    if (node1.node_type == node2.node_type and set(node1.information['Output']) == set(node2.information['Output'])):
        return True
    else:
        return False

def qep_diff_to_natural(qep_diff):
    result = []
    for diff in qep_diff['update']:
        result.append(diff[0].node_type + " with output " + str(diff[0].information['Output']) + " changed to "+ diff[1].node_type + " with output " + str(diff[1].information['Output']) + " due to changes in ___")
    for diff in qep_diff['delete']:
        result.append(diff.node_type + " with output " + str(diff.information['Output']) +  " was removed due to changes in ___")
    for diff in qep_diff['insert']:
        result.append(diff.node_type + " with output " + str(diff.information['Output']) + " was added due to changes in ___")
    return result
