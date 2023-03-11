import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from argparse import ArgumentParser
import json
if __name__=='__main__':
    sc_conf = pyspark.SparkConf()\
        .setAppName('task2')\
        .setMaster('local[*]')\
        .set('spark.drive.memory', '%8g')\
        .set('spark.executor.memory','4g')
    sc = SparkContext(conf= sc_conf)
    sc.setLogLevel("OFF")
    parser = ArgumentParser(description='A1T1')
    parser.add_argument('--review_file', type=str, default= 'review.json', help='the input file review')
    parser.add_argument('--output_file', type = str, default= 'answer1.json', help='the output file contains your answers')
    parser.add_argument('--business_file', type=str, default = 'business.json', help='the input file business')
    parser.add_argument('--n', type = int, default=10, help='top n categories with highest average stars')

    # parser.add_argument('--m', type = int, default = 10, help='top m users')
    # parser.add_argument('--n',  type = int, default = 10, help='top n frequent words')
    args=parser.parse_args()

spark = SparkSession(sc)  
rdd1 = sc.textFile(args.review_file).map(lambda x: json.loads(x))
rdd2 = sc.textFile(args.business_file).map(lambda x: json.loads(x))

rdd_joined = rdd1.map(lambda x: (x["business_id"], x)).join(rdd2.map(lambda x: (x["business_id"], x)))
grouped_by_category = rdd_joined.map(lambda x:(x[0],(x[1][1]['categories'],x[1][0]['stars'])))\
                            .filter(lambda x : x[1][0] is not None)

n = grouped_by_category.map(lambda x:(x[1][0],x[1][1]))
l = n.map(lambda x: (x[0].split(","),x[1]))
new_rdd = l.flatMap(lambda x: [(key.strip(), x[1]) for key in x[0]])
get_ave= new_rdd.map(lambda x: (x[0],(x[1],1)))\
        .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))\
         .map(lambda x: (x[0], x[1][0]/x[1][1]))\
        .sortBy(lambda x: (-x[1], x[0]))
        

s = get_ave.take(args.n)
resultDict = dict((x,y) for x, y in s)
result = [[key,value] for key, value in resultDict.items()]
key = "result"
dictResult = {key:result}
import json
with open(args.output_file, 'w') as convert_file:
    convert_file.write(json.dumps(dictResult))
    convert_file.close()

