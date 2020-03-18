from pyspark import SparkContext
import sys
from csv import reader


sc= SparkContext()
alltrips = sc.textFile(sys.argv[1], 1)
alltrips = alltrips.mapPartitions(lambda x: reader(x))
alltrips_rdd = alltrips.map(lambda line : (str(line[7]), 1))


rdd = alltrips_rdd.reduceByKey(lambda x,y:(x+y)).sortByKey()

def toCSVLine(data):
    return ','.join(str(d) for d in data)

def removeParenthesis(data):
    return ''.join(str(d) for d in data if d not in '(){}<>\'')

lines = rdd.map(toCSVLine)
finalOp = lines.map(removeParenthesis)
finalOp.saveAsTextFile("task2b.out")

