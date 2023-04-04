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

# def query_difference(q1,q2):




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