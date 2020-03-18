from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.types import StructField,StructType,StringType
import sys
from pyspark.sql.functions import  format_string



spark = SparkSession.builder.getOrCreate()
sc=SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
all_trips= StructType([
    StructField("medallion",StringType(),True),
    StructField("hack_license", StringType(),True),
    StructField("vendor_id", StringType(),True),
    StructField("pickup_datetime", StringType(),True),
    StructField("rate_code", StringType(),True),
    StructField("store_and_fwd_flag", StringType(),True),
    StructField("drop_datetime", StringType(),True),
    StructField("passenger_count", StringType(),True),
    StructField("trip_time_in_secs", StringType(),True),
    StructField("trip_distance", StringType(),True),
    StructField("pickup_longitude", StringType(),True),
    StructField("pickup_latitude", StringType(),True),
    StructField("dropoff_longitude", StringType(),True),
    StructField("dropoff_latitude", StringType(),True),
    StructField("payment_type", StringType(),True),
    StructField("fare_amount", StringType(),True),
    StructField("surcharge", StringType(),True),
    StructField("mta_tax", StringType(),True),
    StructField("tip_amount", StringType(),True),
    StructField("tolls_amount", StringType(),True),
    StructField("total_amount", StringType(),True)])
allTrips = spark.read.format('csv').schema(all_trips).options(header='false',inferschema='true').load(sys.argv[1])
allTrips.createOrReplaceTempView("allTrips")
result = spark.sql("select hack_license, count(distinct medallion) as num_taxis_used from allTrips group by hack_license order by hack_license asc")
result.select(format_string('%s, %d',result.hack_license,result.num_taxis_used)).write.save("task3d-sql.out", format="text")
