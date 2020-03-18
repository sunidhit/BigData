from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.types import StructField,StructType,StringType
import sys
from pyspark.sql.functions import  format_string

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
trips = spark.sql("select medallion_type, count(*) as total_trips from fareLicenses group by medallion_type")
total_revenue = spark.sql("select medallion_type, sum(fare_amount) as total_revenue from fareLicenses group by medallion_type")
tip = spark.sql("select medallion_type, sum(tip_amount) as total_tip from fareLicenses group by medallion_type")
df1 = trips.join(total_revenue,["medallion_type"])
df2 = df1.join(tip,["medallion_type"])
finaloutput = df2.select("medallion_type","total_trips", "total_revenue", ((df2.total_tip/df2.total_revenue)*(100/df2.total_trips)).alias("avg_tip_percentage")).sort(df2.medallion_type.asc())
print(finaloutput.show())
finaloutput.select(format_string('%s,%s,%s,%s',finaloutput.medallion_type,finaloutput.total_trips,finaloutput.total_revenue,finaloutput.avg_tip_percentage)).write.save("task4b-sql.out", format="text")