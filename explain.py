import psycopg2
import configparser
from collections import deque
import sqlparse

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
        self.children = []
        self.node_type = information['Node Type']
        del information['Node Type']
        self.information = information
        if 'Plans' in information:
            plans = information['Plans']
            del information['Plans']
            for plan in plans:
                self.children.append(Node(plan))


def printSQL(query):
    
    parsed = sqlparse.parse(query)[1]
    print(parsed)
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
    # print("All keys: ", all_keys)
    for key in all_keys:
        if q1.get(key) is not None and q2.get(key) is not None: 
            diff = set(q1[key]).symmetric_difference(set(q2[key]))
            if diff:
                diff_result[key] = list(diff)
        elif q1.get(key) is None:
            diff_result[key]=q2[key]
        else:
            diff_result[key]=q1[key]
    print("Difference in q2 compared to q1 is:\n", diff_result)
    print()
    for key, value in diff_result.items():
        final_difference_result[key] = {}
        final_difference_result[key]['Q1'] = []
        final_difference_result[key]['Q2'] = []
        if q1.get(key) is not None:
            for element in value:
                if element in q1[key]:
                    # print(key, ":", element)
                    # print("exist in q1")
                    final_difference_result[key]['Q1'].append(element)
                else:
                    # print(key, ":", element)
                    # print("exists in q2")
                    final_difference_result[key]['Q2'].append(element)
        else:
            # print(key, ":", value)
            # print("key doesn't exist in q1, exist in q2")
            for element in value:
                final_difference_result[key]['Q2'].append(element)
            
    print(final_difference_result)
    
    return final_difference_result