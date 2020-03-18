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
trips_zero_gps = allTrips.filter(allTrips.pickup_latitude == 0.0).filter(allTrips.pickup_longitude == 0.0).filter(allTrips.dropoff_longitude == 0.0).filter(allTrips.dropoff_latitude == 0.0)
trips_zero_gps_count = trips_zero_gps.groupBy(trips_zero_gps.medallion).count()
trips_zero_gps_count.sort(trips_zero_gps_count.medallion.asc())
trips_zero_gps_count.createOrReplaceTempView("trips_zero_gps_count")
percentage_trips = spark.sql("select total_trips.medallion as medallion, (trips_zero_gps_count.count/total_trips.count)*100 as percentage_of_trips from total_trips join trips_zero_gps_count where total_trips.medallion = trips_zero_gps_count.medallion order by total_trips.medallion asc")
percentage_trips.select(format_string('%s, %f',percentage_trips.medallion,percentage_trips.percentage_of_trips)).write.save("task3c-sql.out", format="text")