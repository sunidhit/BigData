from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.types import StructField,StructType,StringType
import sys
from pyspark.sql.functions import  format_string,date_format



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
trips_time = spark.sql("select medallion, pickup_datetime, count(*) as num_trips from allTrips group by medallion, pickup_datetime")
trips_time.createOrReplaceTempView("trips_time")
check = spark.sql("select medallion, pickup_datetime, num_trips from trips_time where num_trips > 1 order by medallion, pickup_datetime asc")
check.select(format_string('%s,%s',check.medallion,check.pickup_datetime)).write.save("task3b-sql.out", format="text")