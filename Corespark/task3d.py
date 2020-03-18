from pyspark import SparkContext
from csv import reader
import sys

sc= SparkContext()
alltrips = sc.textFile(sys.argv[1], 1)
alltrips = alltrips.mapPartitions(lambda x: reader(x))

alltrips_rdd = alltrips.map(lambda line : (line[1], line[0]))

rdd = alltrips_rdd.distinct().countByKey()

final_output = sc.parallelize(list(rdd.items())).sortByKey()

def toCSVLine(data):
    return ','.join(str(d) for d in data)

def removeParenthesis(data):
    return ''.join(str(d) for d in data if d not in '(){}<>\'')

lines = final_output.map(toCSVLine)

output = lines.map(removeParenthesis)

output.saveAsTextFile("task3d.out")
