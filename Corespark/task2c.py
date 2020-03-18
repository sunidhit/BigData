from pyspark import SparkContext
import sys
from csv import reader


sc= SparkContext()
alltrips = sc.textFile(sys.argv[1], 1)
alltrips = alltrips.mapPartitions(lambda x: reader(x))


dates = alltrips.map(lambda x: ((x[3].split("-")[0] + "-" + x[3].split("-")[1] + "-" + x[3].split("-")[2].split(" ")[0]),( x[15], x[16], x[18], x[19])))

sumRDD = dates.reduceByKey(lambda x,y :(float(x[0])+float(y[0]),float(x[1])+float(y[1]),float(x[2])+float(y[2]),float(x[3])+float(y[3])))
final_output = sumRDD.mapValues(lambda x: (float(x[0])+float(x[1])+float(x[2]), x[3])).sortByKey()

def toCSVLine(data):
    return ','.join(str(d) for d in data)

def removeParenthesis(data):
    return ''.join(str(d) for d in data if d not in '(){}<>\'')

lines = final_output.map(toCSVLine)
finalOp = lines.map(removeParenthesis)
finalOp.saveAsTextFile("task2c.out")







