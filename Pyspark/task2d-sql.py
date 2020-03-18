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

total_trips = allTrips.groupBy(allTrips.medallion).count()
total_trips.sort(total_trips.medallion.asc())
total_trips.createOrReplaceTempView("total_trips")
medallion_days=allTrips.select('medallion',date_format('pickup_datetime','YYYY-MM-dd').alias('day'))
medallion_days.createOrReplaceTempView("medallion_days")
days_distinct = spark.sql("select medallion, count(distinct day) as days_driven from medallion_days group by medallion")
days_distinct.createOrReplaceTempView("days_distinct")
finaloutput = spark.sql("select total_trips.medallion as medallion, total_trips.count as total_trips, days_distinct.days_driven as days_driven, total_trips.count/days_distinct.days_driven as average from total_trips join days_distinct where total_trips.medallion = days_distinct.medallion order by total_trips.medallion asc")
finaloutput.select(format_string('%s,%s,%s,%f',finaloutput.medallion, finaloutput.total_trips, finaloutput.days_driven, finaloutput.average)).write.save("task2d-sql.out", format="text")