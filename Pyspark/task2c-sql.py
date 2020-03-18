from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string, from_unixtime, unix_timestamp, to_date
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.types import StructField,StructType,StringType
import sys
import pyspark.sql.functions as func

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

df = spark.sql("select date(pickup_datetime) as date, round(sum(fare_amount + surcharge + tip_amount),2) as total_revenue, round(sum(tolls_amount),2) as total_tolls from allTrips group by date(pickup_datetime) order by date(pickup_datetime) asc")

df.select(format_string('%s,%s,%s', from_unixtime(unix_timestamp(df.date, "yyyy-MM-dd"),'yyyy-MM-dd'), func.round(df.total_revenue,2), func.round(df.total_tolls,2))).write.save("task2c-sql.out", format="text")