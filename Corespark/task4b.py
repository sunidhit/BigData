from pyspark import SparkContext
import sys
from csv import reader
from operator import add


sc= SparkContext()
licenses = sc.textFile(sys.argv[1], 1)
licenses = licenses.mapPartitions(lambda x: reader(x))

licenses_rdd = licenses.map(lambda line : (line[18] , (line[5] , line[8] , 1)))

fare_sum =licenses_rdd.reduceByKey(lambda x, y: (float(x[0]) + float(y[0]),\
                                                    float(x[1]) + float(y[1]), x[2]+y[2]))

final_output = fare_sum.mapValues(lambda x: (x[2], x[0], round(100/x[2]*(x[0]/x[1]),2))).sortByKey()

def toCSVLine(data):
    return ','.join(str(d) for d in data)

def removeParenthesis(data):
    return ''.join(str(d) for d in data if d not in '(){}<>\'')

lines = final_output.map(toCSVLine)
finalOp = lines.map(removeParenthesis)
finalOp.saveAsTextFile("task4b.out")
