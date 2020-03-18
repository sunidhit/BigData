
from pyspark import SparkContext
from csv import reader
import sys

sc = SparkContext("local", "q3a")
alltrips = sc.textFile(sys.argv[1], 1)
alltrips = alltrips.mapPartitions(lambda x: reader(x))

alltrips_rdd = alltrips.map(lambda line : (line[1], (line[15])))

invalid_count = alltrips_rdd.filter(lambda x: float(x[1]) < 0).count()

output_rdd = sc.parallelize((invalid_count, ''))

output_rdd.saveAsTextFile("task3a.out")