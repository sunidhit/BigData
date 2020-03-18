from pyspark import SparkContext
from csv import reader
import sys
from operator import add

sc = SparkContext()
fares_rdd = sc.textFile(sys.argv[1], 1)
fares_rdd = fares_rdd.mapPartitions(lambda x: reader(x))


counts05 = fares_rdd.map(lambda x: (x[0], x[15], 1)).filter(lambda x: 0 <= float(x[1]) <= 5)\
    .map(lambda x: ((0,5),x[2])).reduceByKey(add)
counts510 = fares_rdd.map(lambda x: (x[0], x[15], 1)).filter(lambda x: 5 < float(x[1]) <= 15)\
    .map(lambda x: ((5,15),x[2])).reduceByKey(add)
counts1530 = fares_rdd.map(lambda x: (x[0], x[15], 1)).filter(lambda x: 15 < float(x[1]) <= 30)\
    .map(lambda x: ((15,30),x[2])).reduceByKey(add)
counts3050 = fares_rdd.map(lambda x: (x[0], x[15], 1)).filter(lambda x: 30 < float(x[1]) <= 50)\
    .map(lambda x: ((30,50),x[2])).reduceByKey(add)
counts50100 = fares_rdd.map(lambda x: (x[0], x[15], 1)).filter(lambda x: 50 < float(x[1]) <= 100)\
    .map(lambda x: ((50,100),x[2])).reduceByKey(add)
countsGt100 = fares_rdd.map(lambda x: (x[0], x[15], 1)).filter(lambda x: float(x[1]) > 100)\
    .map(lambda x: ((100,' '),x[2])).reduceByKey(add)


def toCSVLine(data):
    return ','.join(str(d) for d in data)

def removeParenthesis(data):
    return ''.join(str(d) for d in data if d not in '(){}<>\'')

rdd_union = counts05.union(counts510).union(counts1530).union(counts3050).union(counts50100).union(countsGt100).sortByKey()

lines = rdd_union.map(toCSVLine)

finalOp = lines.map(removeParenthesis)

finalOp.saveAsTextFile("task2a.out")