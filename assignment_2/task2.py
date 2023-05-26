
import pyspark
from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark import SparkContext
# sc = pyspark.SparkContext(master='local', appName='myAppName',conf=conf)
import time
from itertools import combinations
from collections import Counter
from operator import add
import math

if __name__=='__main__':
    sc_conf = pyspark.SparkConf()\
        .setAppName('task2')\
        .setMaster('local[*]')\
        .set('spark.drive.memory', '%8g')\
        .set('spark.executor.memory','4g')
    sc = SparkContext(conf= sc_conf)
    # sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel("OFF")
    parser = ArgumentParser(description='A1T1')
    # os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
    # os.environ['PYSPARK_PYTHON'] = 'python'
    parser.add_argument('--input_file', type=str, default= 'Sample_data.csv', help='input file')
    parser.add_argument('--output_file', type = str, default= 'answer.json', help='the output ile contains your answers')
    parser.add_argument('--k', type = int, default=10, help='case 1 or case 2')
    parser.add_argument('--s', type = int, default =10, help='support threshold')
    args=parser.parse_args()
    
startTime = time.time()
spark = SparkSession(sc) 
candidatesList = []
freuqntItemList= []
thresholdFilter=args.k
supportVal=args.s
startTime = time.time()
textData = sc.textFile(args.input_file)
data = textData.filter(lambda x: x != "user_id,business_id")
# dataRDD_pre = data.repartition(10)
basicRDD= data.map(lambda x: x.split(","))
def findNewCandidate(basket,basketSupport,previousCandidates,checkSize):
    counter = {}
    if len(previousCandidates) == 0:
        return []
    checkCandidate = []
    newCandidates = []
    for i in range(len(previousCandidates)):
        for j in range(i+1, len(previousCandidates)):
            pair1 = previousCandidates[i]
            pair2 = previousCandidates[j]
            newCandidate = []
            for item in pair1:
                if item not in newCandidate:
                    newCandidate.append(item)
            for item in pair2:
                if item not in newCandidate:
                    newCandidate.append(item)     
            if len(newCandidate) == checkSize+1:
                checkCandidate.append(tuple(sorted(newCandidate)))
                # print(newCandidate)
    # print(checkCandidate[1])
    checkCandidate = set(checkCandidate)
    for item in checkCandidate:
        for item1 in basket:
            checkItem = list(item)
            inBasketItem = list(item1)
            isSubset = True
            for i in range(len(checkItem)):
                if checkItem[i] not in inBasketItem:
                    isSubset = False
                    break
            if isSubset:
                if item not in counter:
                    counter[item] = 1
                else:
                    counter[item] += 1   
    for x in counter:
        if counter[x] >= basketSupport:
            newCandidates.append(x)
    return newCandidates

def Pass1(transactions):
    lst = [0]*300000000
    basket = [x for x in transactions]
    lengthBasket = len(basket)
    basketSupport = lengthBasket*perUnitsupportVal
    singleCandidateCount = {}
    singleCandidates = []
    finalCandidates = []
    for x in basket:
        pairs = list(combinations(x,2))
        for item in x:
            if item not in singleCandidateCount:
                singleCandidateCount[item] = 1
            else:
                singleCandidateCount[item] += 1
        for i in pairs:
            index = hash(tuple(sorted(i)))%300000000#index
            lst[index] += 1
    for x in singleCandidateCount: 
        if singleCandidateCount[x] >= basketSupport:
            singleCandidates.append(tuple([x])) #appending frequent items to singleCand
    dictCountPairs = {}
    for x in basket:
         freqLstPart =[]
         for item in x:
            if tuple([item]) in singleCandidates:
                # if item not in freqLstPart:
                freqLstPart.append(item)
         pairsFreq = list(combinations(freqLstPart,2))
         for i in pairsFreq:
            index = hash(tuple(sorted(i)))%300000000
            if lst[index] >= basketSupport: 
                if tuple(sorted(i)) not in dictCountPairs:
                    dictCountPairs[tuple(sorted(i))] = 1 #i is a tuple
                else:
                    dictCountPairs[tuple(sorted(i))] += 1
    pairListFreq = []
    for i,k in dictCountPairs.items():
        if k >= basketSupport:
            pairListFreq.append(i)
    finalCandidates += singleCandidates
    finalCandidates += pairListFreq
    previousCandidates = pairListFreq 
    checkSize = 2
    while True:
        newCandidates =  findNewCandidate(basket,basketSupport,previousCandidates,checkSize)
        finalCandidates += newCandidates
        previousCandidates = newCandidates
        checkSize += 1
        if(len(previousCandidates)==0):
            break
    return finalCandidates

def Pass2(transactions):
    basket = [x for x in transactions]
    candidates = pass2CandidateList.value
    candidateCount = {}
    for b in basket:
        for k,v in candidates:
            for item in v:
                isSubset = True
                for i in range(len(item)):
                    if item[i] not in b:
                        isSubset = False
                        break
                if isSubset:
                    if item not in candidateCount:
                        candidateCount[item] = 1
                    else:
                        candidateCount[item] += 1

    for item in sorted(candidateCount.keys()):
        yield (item, candidateCount[item])

basketRDD = basicRDD.map(lambda x: (str(x[0]), [str(x[1])])).reduceByKey(lambda x, y: x+y).map(lambda x: x[1]).map(lambda x: list(set(x))).filter(lambda x: len(x) > args.s)
perUnitsupportVal = (1.0 * float(args.s))/basketRDD.count()
tempCandidatesList = basketRDD.mapPartitions(Pass1)
candidatesList = tempCandidatesList.distinct().groupBy(len).collect()
pass2CandidateList = sc.broadcast(candidatesList)
pass2CandidateListRDD = basketRDD.mapPartitions(Pass2).reduceByKey(lambda x, y: x+y)
freuqntItemList=pass2CandidateListRDD.filter(lambda x: x[1] >= args.s)
freuqntItemList=freuqntItemList.map(lambda x: x[0]).groupBy(len).collect()

import json
with open(args.output_file, 'w') as convert_file:
    mydict={}
    for a, b in sorted(candidatesList):
        candidates = [ x for x in b]
        if "Candidates" not in mydict:
            mydict["Candidates"] = [[list(tup) for tup in candidates]]
        else:
            mydict["Candidates"].append([list(tup) for tup in candidates])    
    #     file.write(str(mydict))
    for a, b in sorted(freuqntItemList):
        frequentItemSets = [x for x in b]
        if "Frequent Itemsets" not in mydict:
            mydict["Frequent Itemsets"] = [[list(tup) for tup in frequentItemSets]]
            
        else:
            mydict["Frequent Itemsets"].append([list(tup) for tup in frequentItemSets])    
    endTime = time.time()
    mydict["Runtime"] = endTime-startTime
    convert_file.write(json.dumps(mydict))
    convert_file.close()

    
