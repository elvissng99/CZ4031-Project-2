import psycopg2
import configparser
from collections import deque
# from pprint import pprint

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

class Node:
    def __init__(self,information):
        self.index = None
        self.children = []
        self.node_type = information['Node Type']
        del information['Node Type']
        self.information = information
        if 'Plans' in information:
            plans = information['Plans']
            del information['Plans']
            for plan in plans:
                child = Node(plan)
                self.children.append(child)

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
        path = ["Insert: " + str(tree2.node_type) + str(tree2.index)]
        for child in tree2.children:
            _, child_path = tree_edit_distance(None, child)
            path.extend(child_path)
        return len(path), path

    # if tree2 is empty, delete all the nodes of tree1
    if tree2 is None:
        path = ["Delete: " + str(tree1.node_type) + str(tree1.index)]
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
        path = ["Matched: " + str(tree1.node_type) + str(tree1.index) + " to: " + str(tree2.node_type) + str(tree2.index)]
    # update nodes
    else:
        cost = 1
        path = ["Update: " + str(tree1.node_type) + str(tree1.index) + " to: " + str(tree2.node_type) + str(tree2.index)]

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
    results = tree_edit_distance(tree1, tree2)
    return results

    

        
    





