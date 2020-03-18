from pyspark import SparkContext
from csv import reader
import sys



sc = SparkContext()
trips_rdd = sc.textFile(sys.argv[1], 1)
trips_rdd = trips_rdd.mapPartitions(lambda x: reader(x))


zero_coordinates = trips_rdd.map(lambda x: (x[0], (x[10], x[11], x[12], x[13])))\
    .filter(lambda x: 0.0 in (float(x[1][0]), (float(x[1][1]), float(x[1][2]), float(x[1][3]))))\
    .groupByKey().mapValues(lambda vals: (len(vals))).sortByKey()

zero_coordinates2 = trips_rdd.map(lambda x: (x[0], (x[10], x[11], x[12], x[13])))\
                    .filter(lambda x: 0.0 not in (float(x[1][0]), (float(x[1][1]), float(x[1][2]), float(x[1][3]))))\
                    .groupByKey().mapValues(lambda vals: (0)).sortByKey()

total_medallion = trips_rdd.map(lambda x: (x[0], (x[10], x[11], x[12], x[13])))\
    .groupByKey().map(lambda x: (x[0], (list(x[1]), len(x[1])))).sortByKey()

zero_coordinates = zero_coordinates.union(zero_coordinates2).reduceByKey(lambda x, y: (x+y))

medallion_totals = total_medallion.mapValues(lambda x: x[-1])

final = medallion_totals.join(zero_coordinates).mapValues(lambda x: x[1]/x[0]*100)

def toCSVLine(data):
    return ','.join(str(d) for d in data)

def removeParenthesis(data):
    return ''.join(str(d) for d in data if d not in '(){}<>\'')

lines = final.map(toCSVLine)

finalOp = lines.map(removeParenthesis)

finalOp.saveAsTextFile("task3c.out")

