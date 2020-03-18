from pyspark.sql import SparkSession
from pyspark.sql.functions import  format_string,date_format
from pyspark.sql.functions import *
import sys

spark = SparkSession.builder.appName('joins_example').getOrCreate()
#read file
fare = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
licenses = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])

result = licenses.join(fare, on=['medallion'], how='inner').sort(["medallion","hack_license","pickup_datetime"],ascending=True)
new_pickup_time=date_format(result.pickup_datetime,"yyyy-MM-dd HH:MM:ss")
result = result.withColumn('name', regexp_replace("name",","," "))
result.select(format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s', result.medallion, result.hack_license, result.vendor_id, result.pickup_datetime, result.payment_type, result.fare_amount, result.surcharge, result.mta_tax, result.tip_amount, result.tolls_amount, result.total_amount, result.name, result.type, result.current_status, result.DMV_license_plate, result.vehicle_VIN_number, result.vehicle_type, result.model_year, result.medallion_type, result.agent_number, result.agent_name, result.agent_telephone_number, result.agent_website, result.agent_address, result.last_updated_date, result.last_updated_time)).write.save('task1b-sql.out',format="text")