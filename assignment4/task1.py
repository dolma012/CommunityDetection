import argparse
# import json
import time
import pyspark
# import itertools
# import random
# from pyspark import sql
# from pyspark.sql.functions import col, lit, when
from functools import reduce

# from pyspark.sql.functions import col, lit, when
# import numpy 
from graphframes import GraphFrame
# pyspark.sql.SQLContext(sparkContext, sqlContext=None)¶
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
    # parser.add_argument('--community_output_file', type=str, default='./result.txt', help='the output file contains your answers')
    parser.add_argument('--input_file', type=str, default='./hw4/sample.csv')
    parser.add_argument('--community_output_file', type=str, default='out.txt')
    parser.add_argument('--filter_threshold', type=int, default=7)
    args = parser.parse_args()

spark = SparkSession(sc)  
input_rdd = args.input_file
output = args.community_output_file
thresh = args.filter_threshold
user_rdd = sc.textFile(input_rdd)
vertices = user_rdd.filter(lambda row: row != "user_id,business_id")
group_by_user = vertices.map(lambda row : row.split(",")).groupBy(lambda x: x[0])
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

            
e = [[]]
key_lst = []
for key in key_list:
    if len(edge_list[key]) > 0:
        for val in edge_list[key]:
            temp = [key,val]
            e.append(temp)
            if key not in key_lst:
                key_lst.append(key)
            if val not in key_lst:
                key_lst.append(val)
e_rdd = sc.parallelize(e)
e_rdd = e_rdd.filter(lambda x: x != [])  # skip the header row
v_rdd = sc.parallelize(key_lst)
v_rdd = v_rdd.map(lambda x: [x])
v = v_rdd.toDF(["id"])
e= e_rdd.toDF(["src", "dst"])
graph = GraphFrame(v, e)
print("Graph here")

communities = graph.labelPropagation(maxIter=5)
communities_rdd = communities.rdd
comm_rdd = communities_rdd.map(lambda x: (x[1], x[0]))
# print(comm_rdd)
communities = comm_rdd.groupByKey().mapValues(list).map(lambda x: x[1]).collect()
print(communities)

resultDict = {}
for community in communities:
    community = list(map(lambda userId: "'" + userId + "'", sorted(community)))
    community = ", ".join(community)
    if len(community) not in resultDict:
        resultDict[len(community)] = []
    resultDict[len(community)].append(community)
results = list(resultDict.items())
results.sort(key = lambda pair: pair[0])
# print(resultDict)
output = open(output, "w")
for result in results:
    resultList = sorted(result[1])
    for community in resultList:
        output.write(community + "\n")
sc.stop()
output.close()


