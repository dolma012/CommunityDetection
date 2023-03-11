import pyspark
from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark import SparkContext
# conf = pyspark.SparkConf().set('spark.driver.host','127.0.0.1')
# sc = pyspark.SparkContext(master='local', appName='myAppName',conf=conf)
if __name__=='__main__':
    sc_conf = pyspark.SparkConf()\
        .setAppName('task1')\
        .setMaster('local[*]')\
        .set('spark.drive.memory', '%8g')\
        .set('spark.executor.memory','4g')

    sc = SparkContext(conf= sc_conf)
    # sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    parser = ArgumentParser(description='A1T1')
    # os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
    # os.environ['PYSPARK_PYTHON'] = 'python'
    parser.add_argument('--input_file', type=str, default= 'review.json', help='input file')
    parser.add_argument('--output_file', type = str, default= 'answer.json', help='the output ile contains your answers')
    parser.add_argument('--stopwords', type=str, default = 'stopwords', help='the file contains all your stopwords')
    parser.add_argument('--y', type = int, default=2018, help='year')
    parser.add_argument('--m', type = int, default = 10, help='top m users')
    parser.add_argument('--n',  type = int, default = 10, help='top n frequent words')
    args=parser.parse_args()


spark = SparkSession(sc)  
# spark = SparkSession.builder.appName("Read JSON File").getOrCreate()
textdf = spark.read.json(args.input_file)
rdd = textdf.rdd
dict_final = {}
num_rows = rdd.count()
dict_a = {"A":num_rows}
mapped_df = rdd.filter(lambda row: int(row[1][0:4]) == args.y)
review_ids = mapped_df.map(lambda x: x.review_id).distinct()
count = review_ids.count()
dict_a.update({"B":count})
from pyspark.sql.functions import count
col2_values = rdd.map(lambda x: x[5])
num_unique = col2_values.distinct().count()
dict_a.update({"C":num_unique})
from pyspark.sql.functions import desc
from pyspark.sql.functions import count
pair_rdd = rdd.map(lambda row: (row[5], 1)) #counter
# dict_rdd = dict((k,v) for k,v in pair_rdd)
# Group by user_id to count the number of reviews per user
grouped_rdd = pair_rdd.reduceByKey(lambda a, b: a + b)
# collected_rdd = grouped_rdd.collect()
# grouped_rdd.take(10)
sorted_rdd = grouped_rdd.sortBy(lambda x: -x[1])
take_rdd = sorted_rdd.take(args.m)
dict_collect = dict((x,y) for x,y in take_rdd)
result = [[key,value] for key, value in dict_collect.items()]
final = {"D":result}
dict_a.update(final)

rdd_stop= sc.textFile(args.stopwords)
rdd_collected = rdd_stop.collect()
freq_rdd = rdd.map(lambda row: row[4])
flat_rdd = freq_rdd.map(lambda row: row.strip())
each_rdd = flat_rdd.flatMap(lambda x:x.split("\n\n"))
one_rdd= each_rdd.flatMap(lambda x:x.split("\n"))
k = one_rdd.flatMap(lambda row : row.split(" "))
def lower_clean_str(x):
    punc ='!#$%&\()*+,-./:;""<=>?@[\\]^_`{|}~'
    lowercased_str = x.lower()
    for ch in punc:
        lowercased_str = lowercased_str.replace(ch,'')
    return lowercased_str
remove_rdd = k.map(lower_clean_str)
clean_data_rdd = remove_rdd.filter(lambda row:row.lower() not in rdd_collected and len(row)>0)
count_rdd = clean_data_rdd.map(lambda x:( x, 1))
group_rdd = count_rdd.reduceByKey(lambda a, b: a + b)
sorted_rdd1 = group_rdd.sortBy(lambda x: -x[1])
top_words = sorted_rdd1.take(args.n)
resultDict = dict((x,y) for x, y in top_words)
result = [key for key in resultDict.keys()]
dict_a.update({"E":result})
# print(dict_a)
# f = open(args.output_file, "w")
import json
with open(args.output_file, 'w') as convert_file:
    convert_file.write(json.dumps(dict_a))
    convert_file.close()

























