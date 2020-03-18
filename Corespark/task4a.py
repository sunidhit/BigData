import sys
from pyspark import SparkContext
from csv import reader

sc = SparkContext("local", "q4a")
licence_trips_rdd = sc.textFile(sys.argv[1], 1)
licence_trips_rdd = licence_trips_rdd.mapPartitions(lambda x: reader(x))

licence_trips_rdd = licence_trips_rdd.map(lambda x: (x[16], (x[5], x[8], 1)))



final = licence_trips_rdd.reduceByKey(lambda x, y: (float(x[0]) + float(y[0]),\
                                                    float(x[1]) + float(y[1]), x[2]+y[2]))

final_op = final.mapValues(lambda x: (x[2], x[0], round(100/x[2]*(x[0]/x[1]),2))).sortByKey()

def toCSVLine(data):
    return ','.join(str(d) for d in data)

def removeParenthesis(data):
    return ''.join(str(d) for d in data if d not in '(){}<>\'')

lines = final_op.map(toCSVLine)

finalOp = lines.map(removeParenthesis)

finalOp.saveAsTextFile("task4a.out")