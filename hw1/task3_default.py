import pyspark
conf = pyspark.SparkConf().set('spark.driver.host','127.0.0.1')
# sc = pyspark.SparkContext(master='local', appName='myAppName',conf=conf)
from pyspark.sql import SparkSession
from pyspark import SparkContext
from argparse import ArgumentParser
if __name__=='__main__':
    sc_conf = pyspark.SparkConf()\
        .setAppName('task3')\
        .setMaster('local[*]')\
        .set('spark.drive.memory', '20g')\
        .set('spark.executor.memory','20g')
    sc = SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    parser = ArgumentParser(description='A1T1')
    parser.add_argument('--input_file', type=str, default= 'review.json', help='the input file review')
    parser.add_argument('--output_file', type = str, default= 'alt3.json', help='the output file contains your answers')
    parser.add_argument('--n',  type = int, default = 10, help='n')
    args=parser.parse_args()
spark = SparkSession(sc)  

review =spark.read.json(args.input_file).rdd
review_key_rdd = review.map(lambda x: (x.business_id, x.review_id))
count_rdd= review_key_rdd.map(lambda x :(x[0], 1))
summed_reviews= count_rdd.reduceByKey(lambda a, b: a + b )
filter_rdd = summed_reviews.filter(lambda x: x[1] > args.n)
def count_in_a_partition(idx, iterator):
    count =0
    for _ in iterator:
        count+=1
    return idx, count
indexList =  review.mapPartitionsWithIndex(count_in_a_partition).collect()
newList =[]#this newList has the correct values of the partition element s
resultDict = {"n_partitions": int(format(review.getNumPartitions()))}
for i in range(0,len(indexList)):
    if i ==1:
        newList.append(indexList[i])
    elif  i%2!=0 and i!=0:
        newList.append(indexList[i])
resultDict.update({"n_items": newList})
# sorted_rdd=summed_reviews.sortBy(lambda x: -x[1])
take_rdd = filter_rdd.collect()
dict_collect = dict((x,y) for x,y in take_rdd)
result = [[key,value] for key, value in dict_collect.items()]
final = {"result":result}
resultDict.update(final)

import json
with open(args.output_file, 'w') as convert_file:
    convert_file.write(json.dumps(resultDict))
    convert_file.close()





