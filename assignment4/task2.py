import argparse
import time
import pyspark
import itertools
import random
import copy
import numpy as np
from collections import defaultdict, deque
from pyspark.sql import SparkSession
if __name__ == '__main__':
    startTime = time.time()
    sc_conf = pyspark.SparkConf() \
        .setAppName('hw4_task1') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '4g') \
        .set('spark.executer.memory', '4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    parser = argparse.ArgumentParser(description='A1T1')
    # parser.add_argument('--betweeness_output_file', type=str, default='./result.txt', help='the output file contains your answers')
    parser.add_argument('--input_file', type=str, default='ub_sample_data.csv')
    parser.add_argument('--community_output_file', type=str, default='out.txt')
    parser.add_argument('--betweenness_output_file', type=str, default='out1.txt')
    parser.add_argument('--filter_threshold', type=int, default=7)
    args = parser.parse_args()
spark = SparkSession(sc)  
input_rdd = args.input_file
output = args.community_output_file
thresh = args.filter_threshold
user_rdd = sc.textFile(input_rdd)
edges = user_rdd.filter(lambda row: row != "user_id,business_id")
group_by_user = edges.map(lambda row : row.split(",")).groupBy(lambda x: x[0])
user_review_list_rdd = group_by_user.flatMap(lambda x: [i for i in x[1]])
grouped_rdd = user_review_list_rdd.map(lambda x: (x[0], [x[1]])) \
                    .reduceByKey(lambda x, y: x + y)
set_rdd = grouped_rdd.map(lambda business: (business[0], business[1])).collectAsMap()
edge_list =  {x:list() for x in set_rdd.keys()}
key_list = list(set_rdd.keys()) #userID

for i in key_list: #actual keys 
    for edge_key in key_list: #get dict edge
        if i != edge_key: 
            sim = len(set(set_rdd[i]).intersection(set(set_rdd[edge_key]))) #checking the intersection length
            if sim >= thresh:
                lst = edge_list[i]
                lst.append(edge_key) #i is key
                edge_list.update({i :lst})           
edges = [[]]
keylst = []
for key in key_list:
    if len(edge_list[key]) > 0:
        for val in edge_list[key]:
            temp = sorted([key,val])
            if temp not in edges:
                edges.append(temp)
            if key not in keylst:
                keylst.append(key)
            if val not in keylst:
                keylst.append(val)

key_lst = sorted(keylst)
num_edges=len(edges)
from collections import defaultdict

def edge_val(root, edges):
    parent = defaultdict(list)
    depth = defaultdict(int)
    shortest_path_count = defaultdict(float)
    credit = defaultdict(float)
    to_expand = []
    bfs_to_expand = [root]
    parent[root] = None
    depth[root] =0
    shortest_path_count[root] = 1
    for c in edges[root]:
        parent[c] = [root]
        depth[c]  = 1
        shortest_path_count[c] =1 
        to_expand.append(c)
        bfs_to_expand.append(c)
    while to_expand:
        node = to_expand.pop(0)
        credit[node] = 1
        paths = sum(shortest_path_count[p] for p in parent[node])
        shortest_path_count[node] = paths
        for edge_pair in edges[node]:
            if edge_pair not in bfs_to_expand:
                to_expand.append(edge_pair)
                parent[edge_pair] = [node]
                bfs_to_expand.append(edge_pair)
                depth[edge_pair] = depth[node] + 1
            elif depth[edge_pair] == depth[node] + 1:
                parent[edge_pair].append(node)
    bfs_to_expand.reverse()
    bfs_to_expand.remove(root)
    val ={}
    for child in bfs_to_expand:
        for parnt in parent[child]:
            score = credit[child] * (shortest_path_count[parnt] / shortest_path_count[child])
            credit[parnt] += score
            val[tuple(sorted([child, parnt]))]= score
    # print(val)
    return list(val.items())

edges.remove(edges[0])
final_edges = edges
edges = sc.parallelize(final_edges)\
    .map(lambda x:tuple(x))\
    .flatMap(lambda x: [(x[0],x[1]), (x[1],x[0])])\
    .groupByKey().mapValues(set).collectAsMap()

com_edges = sc.parallelize(final_edges)\
    .map(lambda x:tuple(x))\
    .flatMap(lambda x: [(x[0],x[1]), (x[1],x[0])])\
    .groupByKey().mapValues(set).collectAsMap()

com_degrees= dict((x,len(y)) for x,y in com_edges.items())
vertices = sorted(com_edges.keys())

betweeness_lst = sc.parallelize(edges.keys())\
    .flatMap(lambda vertex: edge_val(vertex, edges))\
    .reduceByKey(lambda x, y: x + y)\
    .map(lambda x: (x[0], x[1] / 2))

sorted_lst = betweeness_lst.map(lambda x: list(x))\
    .map(lambda x: (tuple(sorted(x[0])), x[1]))\
    .sortBy(lambda x:x[0])\
    .sortBy(lambda x:x[1],ascending=False).collect()

output = open(args.betweenness_output_file, 'w')
for betweeness in sorted_lst:
    output.write(str(betweeness[0]) + ', ' + str(betweeness[1])+ '\n')

def remove_edge(edges_dict, edge_tuple):
    remove_i = edge_tuple[0][1]
    remove_j = edge_tuple[0][0]
    copied_edge_dict = copy.deepcopy(edges_dict)
    copied_edge_dict[remove_i].remove(remove_j)
    copied_edge_dict[remove_j].remove(remove_i)
    return copied_edge_dict

def update_communities(edge_dict):
    # Create a copy of the original edges dictionary
    copied_edge_dict = copy.deepcopy(edge_dict)
    communities = []
    visited = set()
    for node in sorted(copied_edge_dict.keys()):
        if node not in visited:
            # Initialize a new community with the current node
            new_community = set([node])
            visited.add(node)
            # Traverse the graph to find all connected nodes
            stack = list(copied_edge_dict[node])
            while stack:
                curr_node = stack.pop()
                if curr_node not in visited:
                    new_community.add(curr_node)
                    visited.add(curr_node)
                    stack.extend(copied_edge_dict[curr_node])   
            # Add the new community to the list of communities
            communities.append(new_community)
    return communities


def compute_modularity(adj_matrix, communities, com_degrees):
    m = np.sum(adj_matrix) / 2
    q = 0
    # print(m)
    for comm in communities:
        for i in range(len(comm)):
            list_comm = sorted(list(comm))
            for j in range(len(comm)):
                if i == j:
                    k_i = com_degrees[list_comm[i]]
                    k_j = k_i
                else:
                    k_i = com_degrees[list_comm[i]]
                    k_j = com_degrees[list_comm[j]]
                idx_i = key_lst.index(list_comm[i])
                idx_j = key_lst.index(list_comm[j])
                # print("degree i: ", k_i, "degree j", k_j)
                q += (adj_matrix[idx_i][idx_j] - (k_i * k_j)/ (2 * m))
    q = q / (2 * m)
    return q

#initialize adjacency matrix
matrix = np.zeros((len(key_lst), len(key_lst)))
sorted_keys = sorted(com_edges.keys())

for edge in sorted_keys:
    for pair in sorted(com_edges[edge]):
        i = key_lst.index(edge)
        j = key_lst.index(pair)
        if matrix[i,j] == 0:
            matrix[i,j] = 1
# print(matrix)

max_val = -1
modify_betwn = copy.deepcopy(sorted_lst)
k = len(sorted_lst)
to_modify = key_lst
communities=[]
com_degrees= dict((x,len(y)) for x,y in com_edges.items())
while k >0:
    edge = max(modify_betwn, key=lambda x: x[1])
    modify_betwn.pop(modify_betwn.index(edge))
    updated_edges = remove_edge(com_edges,edge)
    betweeness_lst = sc.parallelize(updated_edges.keys())\
            .flatMap(lambda vertex: edge_val(vertex, updated_edges))\
            .reduceByKey(lambda x, y: x + y)\
            .map(lambda x: (x[0], x[1] / 2))
    modify_betwn = betweeness_lst.map(lambda x: list(x))\
        .map(lambda x: (tuple((x[0])), x[1])).collect()
    communities = update_communities(updated_edges)
    val = compute_modularity(matrix, communities, com_degrees)
    # print(val)
    if val > max_val:
        max_val = val
        final_communities = communities
    k-=1
    com_edges = updated_edges

print(max_val)
resultDict = {}
for community in final_communities:
    community = list(map(lambda userId: "'" + userId + "'", (community)))
    community = ", ".join(community)
    if len(community) not in resultDict:
        resultDict[len(community)] = []
    resultDict[len(community)].append(community)
results = list(resultDict.items())
results.sort(key = lambda pair: pair[0])
# print(resultDict)
output = open(args.community_output_file, "w")
for result in results:
    resultList = sorted(result[1])
    # print(result)
    for community in resultList:
        output.write(community + "\n")
sc.stop()
output.close()
resultDict = {}
for community in final_communities:
    community = list(map(lambda userId: "'" + userId + "'", sorted(community)))
    community = ", ".join(community)
    if len(community) not in resultDict:
        resultDict[len(community)] = []
    resultDict[len(community)].append(community)

results = list(resultDict.items())
results.sort(key = lambda pair: pair[0])
output = open(args.community_output_file, "w")
for result in results:
    resultList = sorted(result[1])
    for community in resultList:
        output.write(community + "\n")

endTime = time.time()
print(endTime-startTime)