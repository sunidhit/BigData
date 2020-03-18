import sys
from pyspark import SparkContext
from csv import reader


sc = SparkContext()
trips_rdd = sc.textFile(sys.argv[1], 1)
trips_rdd = trips_rdd.mapPartitions(lambda x: reader(x))


dates = trips_rdd.map(lambda x: (x[0], x[3]))\
           .map(lambda x: (x[0], x[1].split("-")[0] + "-" + x[1].split("-")[1] + "-" + x[1].split("-")[2].split(" ")[0]))\
            .groupByKey().mapValues(lambda vals: (len(vals), len(set(vals)), len(vals)/len(set(vals)))).sortByKey()

def toCSVLine(data):
    return ','.join(str(d) for d in data)

def removeParenthesis(data):
    return ''.join(str(d) for d in data if d not in '(){}<>\'')

lines = dates.map(toCSVLine)

result = lines.map(removeParenthesis)

result.saveAsTextFile("task2d.out")