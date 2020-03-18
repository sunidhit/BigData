from pyspark.sql import SparkSession
from pyspark.sql.functions import  format_string,date_format
import sys

spark = SparkSession.builder.appName('joins_example').getOrCreate()
#read file
fare = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])
trips = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])

#print(fare.show())
all_trips = trips.join(fare, on=['medallion','hack_license','vendor_id','pickup_datetime'], how='inner').sort(["medallion","hack_license","pickup_datetime"],ascending=True)


#do date_fromatting
new_pickup_time=date_format(all_trips.pickup_datetime,"yyyy-MM-dd HH:MM:ss")
new_drop_time=date_format(all_trips.dropoff_datetime,"yyyy-MM-dd HH:MM:ss")
all_trips.select(format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s', all_trips.medallion, all_trips.hack_license, all_trips.vendor_id, new_pickup_time, all_trips.rate_code, all_trips.store_and_fwd_flag, new_drop_time, all_trips.passenger_count, all_trips.trip_time_in_secs, all_trips.trip_distance, all_trips.pickup_longitude, all_trips.pickup_latitude, all_trips.dropoff_longitude, all_trips.dropoff_latitude, all_trips.payment_type, all_trips.fare_amount, all_trips.surcharge, all_trips.mta_tax, all_trips.tip_amount, all_trips.tolls_amount, all_trips.total_amount)).write.save('task1a-sql.out',format="text")

