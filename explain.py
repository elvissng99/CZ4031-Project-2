import psycopg2
import configparser
from collections import deque
import sqlparse
import graphviz
from pprint import pprint

class Node:
    def __init__(self,information, parent=None):
        self.parent = parent
        self.index = None
        self.reasons = []
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

def QEP_dfs(root,name,d):
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

def QEP_bfs(root):
    q = deque()
    cur = root
    q.append(cur)
    currentLevelNodes = 1
    nextLevelNodes = 0

    while(len(q) > 0):
        node = q.popleft()
        currentLevelNodes -= 1
        print(node.node_type,len(node.children),end ="\t")
        for child in node.children:
            q.append(child)
            nextLevelNodes+=1
        if(currentLevelNodes==0):
            print()
            currentLevelNodes = nextLevelNodes
            nextLevelNodes = 0


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
    final_difference_result={}
    q1_set = set(q1.keys())
    q2_set = set(q2.keys())
    all_keys = q1_set.union(q2_set)
    for key in all_keys:
        if q1.get(key) is not None and q2.get(key) is not None:
            get_subquery_info(q1,key)
            get_subquery_info(q2,key)
    updated_all_keys = set(q1.keys()).union(set(q2.keys()))
    for key in updated_all_keys:
        if q1.get(key) is not None and q2.get(key) is not None:
            diff = set(q1[key]).symmetric_difference(set(q2[key]))
            if diff:
                diff_result[key] = list(diff)
        elif q1.get(key) is None:
            diff_result[key]=q2[key]
        else:
            diff_result[key]=q1[key]
    print()
    print("Difference in q2 compared to q1 is:\n", diff_result)
    print()
    for key, value in diff_result.items():
        final_difference_result[key] = {}
        final_difference_result[key]['Q1'] = []
        final_difference_result[key]['Q2'] = []
        if q1.get(key) is not None:
            for element in value:
                if element in q1[key]:
                    final_difference_result[key]['Q1'].append(element)
                else:
                    final_difference_result[key]['Q2'].append(element)
        else:
            for element in value:
                final_difference_result[key]['Q2'].append(element)
            
    print(final_difference_result)
    
    return final_difference_result

def get_subquery_info(query,key):
    for value in query[key]:
        if isinstance(value,dict):
            query['subquery'] = list(value.items())
            query[key].remove(value)

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
    if tree1.node_type == tree2.node_type:
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
    qep_diff_length,qep_diff = tree_edit_distance(tree1, tree2)
    qep_diff.reverse()
    return qep_diff_length,qep_diff

def diff_to_natural_language(qep_diff,query_diff):
    result = []
    for diff in qep_diff:
        if "Matched" not in diff:
            if "Update" in diff:
                link = qep_diff_link_to_query_diff(diff[6],query_diff)
                diff_string = diff[6].node_type + " with output " + str(diff[6].information['Output']) + " changed to "+ diff[7].node_type + " with output " + str(diff[7].information['Output'])
                # pprint(link)
            elif "Delete" in diff:
                link = qep_diff_link_to_query_diff(diff[3],query_diff)
                # pprint(link)
                diff_string = diff[3].node_type + " with output " + str(diff[3].information['Output']) +  " was removed."
            elif "Insert" in diff:
                link = qep_diff_link_to_query_diff(diff[3],query_diff)
                # pprint(link)
                diff_string = diff[3].node_type + " with output " + str(diff[3].information['Output']) + " was added."
            else:
                print("SHOULD NOT HAPPEN")
            if 'previous' in link:
                diff_string += "The change is possibly influenced by the child nodes where there were relevant changes in the "
                for i in range(len(link['previous'])):
                    diff_string += link['previous'][i].upper()
                    if i != (len(link['previous']) -1):
                        diff_string += ', '
                diff_string += " clauses."
            
            if(diff_string[-8::] == 'clauses.' and len(link) > 1):
                diff_string += "Additionally, there were relevant changes found in the following clauses."
            elif diff_string[-8::] != 'clauses.' and len(link) > 0:
                diff_string += "This is due to changes in the following clauses."


            for key, value in link.items():
                if key != 'previous':
                    diff_string += " In the " + key.upper() + " clause, "
                    if len(value['Q1'])> 0:
                        diff_string += "query 1 had "
                        for q1_diff in value['Q1']:
                            diff_string += q1_diff + ", "
                        diff_string += "while query 2 did not"
                    if len(value['Q2'])> 0:
                        diff_string += " and query 2 had "
                        for q2_diff in value['Q2']:
                            diff_string += q2_diff + ", "
                        diff_string += "while query 1 did not"
                    diff_string += '.'
            result.append(diff_string)
        else:
            for child in diff[6].children:
                for reason in child.reasons:
                    if reason not in diff[6].reasons:
                        diff[6].reasons.append(reason)
    return result

def qep_diff_link_to_query_diff(diff,query_diff):
    result = {}
    if diff.node_type == "Sort":
        #check for order by differences
        if 'order by' in query_diff:
            result['order by'] = query_diff['order by']

        
    if diff.node_type == "Aggregate" or diff.node_type == "Group":
        aggregation_list = ['count','sum','avg','min','max','array_agg','string_agg','group_concat','rank','dense_rank','row_number']
        #check for select and group by differences
        for q, differences in query_diff['select'].items():
            for difference in differences:
                for aggregation in aggregation_list:
                    aggregation += '('
                    if aggregation in difference:
                        if 'select' not in result:
                            result['select'] = {'Q1':[],'Q2':[]}
                            diff.reasons.append('select')
                        if difference not in result['select'][q]:
                            result['select'][q].append(difference)
                        
        for q, differences in query_diff['group by'].items():
            for difference in differences:
                for attribute in diff.information['Output']:
                    filtered = attribute[attribute.index('.')+1:]
                    if filtered in difference:
                        if 'group by' not in result:
                            result['group by'] = {'Q1':[],'Q2':[]}
                            diff.reasons.append('group by')
                        if difference not in result['group by'][q]:
                            result['group by'][q].append(difference)
                        

    #code from here onwards should link qep differences to the HAVING and WHERE clause differences
    if len(diff.children) !=0:
        for child in diff.children:
            if len(child.reasons)>0:
                if 'previous' not in result:
                    result['previous'] = []
                for reason in child.reasons:
                    if reason not in diff.reasons:
                        diff.reasons.append(reason)
                    if reason not in result['previous']:
                        result['previous'].append(reason)
    if 'previous' not in result:
        for keyword, differences_q1q2_dict in query_diff.items():
            for attribute1 in diff.information['Output']:
                filtered = attribute1[attribute1.index('.')+1:]
                for attribute2 in differences_q1q2_dict['Q1']:
                    if filtered in attribute2:
                        if keyword not in result:
                            result[keyword] = {'Q1':[],'Q2':[]}
                            diff.reasons.append(keyword)
                        if attribute2 not in result[keyword]['Q1']:
                            result[keyword]['Q1'].append(attribute2)

                for attribute2 in differences_q1q2_dict['Q2']:
                    if filtered in attribute2:
                        if keyword not in result:
                            result[keyword] = {'Q1':[],'Q2':[]}
                            diff.reasons.append(keyword)
                        if attribute2 not in result[keyword]['Q2']:
                            result[keyword]['Q2'].append(attribute2)
    return result  
        
