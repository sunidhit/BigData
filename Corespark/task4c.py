from pyspark import SparkContext
import sys
from csv import reader


sc= SparkContext()
licenses = sc.textFile(sys.argv[1], 1)
licenses = licenses.mapPartitions(lambda x: reader(x))



licenses_rdd = licenses.map(lambda line : (line[20] , (line[5])))
fare_sum = licenses_rdd.reduceByKey(lambda x, y: (float(x)+float(y)))
final_output = fare_sum.takeOrdered(10,key=lambda x:-x[1])
final_output_rdd=sc.parallelize(final_output)
print(final_output_rdd.collect())

def toCSVLine(data):
    return ','.join(str(d) for d in data)

def removeParenthesis(data):
    return ''.join(str(d) for d in data if d not in '(){}<>\'')

lines = final_output_rdd.map(toCSVLine)
finalOp = lines.map(removeParenthesis)
finalOp.saveAsTextFile("task4c.out")




