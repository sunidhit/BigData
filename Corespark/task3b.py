from pyspark import SparkContext
from csv import reader
import sys

sc= SparkContext()
alltrips = sc.textFile(sys.argv[1], 1)
alltrips = alltrips.mapPartitions(lambda x: reader(x))
alltrips_rdd = alltrips.map(lambda line : ((line[0],line[3]), (line[2],line[1] )))

#record having same time
rdd = alltrips_rdd.countByKey()
record_rdd = sc.parallelize(list(rdd.items()))
output = record_rdd.filter(lambda x: (int(x[1])>1)).sortByKey()

final_output = output.map(lambda x:x[0])

def toCSVLine(data):
    return ','.join(str(d) for d in data)

def removeParenthesis(data):
    return ''.join(str(d) for d in data if d not in '(){}<>\'')

lines = final_output.map(toCSVLine)
finalOp = lines.map(removeParenthesis)
finalOp.saveAsTextFile("task3b.out")
