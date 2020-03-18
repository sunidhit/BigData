from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.types import StructField,StructType,StringType
import sys
from pyspark.sql.functions import  format_string
import pyspark.sql.functions as func

spark = SparkSession.builder.getOrCreate()
sc=SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
fare_licenses= StructType([
    StructField("medallion",StringType(),True),
    StructField("hack_license", StringType(),True),
    StructField("vendor_id", StringType(),True),
    StructField("pickup_datetime", StringType(),True),
    StructField("payment_type", StringType(),True),
    StructField("fare_amount", StringType(),True),
    StructField("surcharge", StringType(),True),
    StructField("mta_tax", StringType(),True),
    StructField("tip_amount", StringType(),True),
    StructField("tolls_amount", StringType(),True),
    StructField("total_amount", StringType(),True),
    StructField("name", StringType(),True),
    StructField("type", StringType(),True),
    StructField("current_status", StringType(),True),
    StructField("DMV_license_plate", StringType(),True),
    StructField("vehicle_VIN_number", StringType(),True),
    StructField("vehicle_type", StringType(),True),
    StructField("model_year", StringType(),True),
    StructField("medallion_type", StringType(),True),
    StructField("agent_number", StringType(),True),
    StructField("agent_name", StringType(),True),
    StructField("agent_telephone_number", StringType(), True),
    StructField("agent_website", StringType(), True),
    StructField("agent_address", StringType(), True),
    StructField("last_updated_date", StringType(), True),
    StructField("last_updated_time", StringType(), True)])
fareLicenses = spark.read.format('csv').schema(fare_licenses).options(header='false',inferschema='true').load(sys.argv[1])
fareLicenses.createOrReplaceTempView("fareLicenses")
total_revenues = spark.sql("select agent_name, sum(fare_amount) as total_revenue from fareLicenses group by agent_name")
final_output = total_revenues.sort(total_revenues.total_revenue.desc(),total_revenues.agent_name.asc()).limit(10)
final_output.select(format_string('%s,%f',final_output.agent_name,final_output.total_revenue)).write.save("task4c-sql.out", format="text")