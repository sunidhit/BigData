from pyspark import SparkContext
import sys
from csv import reader


sc= SparkContext()
fares = sc.textFile(sys.argv[2], 1)
fares = fares.mapPartitions(lambda x: reader(x))
Trips = sc.textFile(sys.argv[1], 1)
Trips = Trips.mapPartitions(lambda x: reader(x))

# remove headers from file :

fares_header = fares.first()
Trips_header = Trips.first()
fares_rdd = fares.filter(lambda line : line != fares_header)
Trips_rdd = Trips.filter(lambda line : line != Trips_header)


#fares_rdd.collect()
#Trips_rdd.collect()

fares_rdd = fares_rdd.map(lambda line : (line[0] + "," + line[1] + "," + line[2] + "," + \
line[3], line[4] + "," + line[5] + "," + line[6] + "," + line[7] + "," + line[8] + "," + line[9] + "," + line[10]))

Trips_rdd = Trips_rdd.map(lambda line : (line[0] + "," + line[1] + "," + line[2] + "," + \
line[5], line[3] + "," + line[4] + "," + line[6] + "," + line[7] + "," + line[8] + "," + line[9] + "," + line[10] + ","\
+ line[11] + "," + line[12] + "," + line[13]))


rdd=fares_rdd.join(Trips_rdd).sortBy(lambda x : (x[0][0],x[0][1],x[0][3]))

def toCSVLine(data):
    return ','.join(str(d) for d in data)

def removeParenthesis(data):
    return ''.join(str(d) for d in data if d not in '(){}<>\'')

lines = rdd.map(toCSVLine)
finalOp = lines.map(removeParenthesis)
finalOp.saveAsTextFile("task1a.out")

