from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.types import StructField,StructType,StringType
import sys
from pyspark.sql import Row
from pyspark.sql.functions import  format_string

spark = SparkSession.builder.getOrCreate()
sc=SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
all_trips= StructType([
    StructField("medallion",StringType(),True),
    StructField("hack_license", StringType(),True),
    StructField("vendor_id", StringType(),True),
    StructField("pickup_date", StringType(),True),
    StructField("rate_code", StringType(),True),
    StructField("store_and_fwd_flag", StringType(),True),
    StructField("drop_date", StringType(),True),
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

group_data = allTrips.groupBy(allTrips.fare_amount).count()
group_data.createOrReplaceTempView("group_data")
new_row1 = spark.sql("select sum(count) as num_trips from group_data where fare_amount >=0 and fare_amount <=5").collect()
newdf1 = sc.parallelize([Row(amount_range="0,5",num_trips=new_row1[0].num_trips)]).toDF()
new_row2 = spark.sql("select sum(count) as num_trips from group_data where fare_amount > 5 and fare_amount <=15").collect()
newdf2 = sc.parallelize([Row(amount_range="5,15",num_trips=new_row2[0].num_trips)]).toDF()
new_row3 = spark.sql("select sum(count) as num_trips from group_data where fare_amount > 15 and fare_amount <=30").collect()
newdf3 = sc.parallelize([Row(amount_range="15,30",num_trips=new_row3[0].num_trips)]).toDF()
new_row4 = spark.sql("select sum(count) as num_trips from group_data where fare_amount > 30 and fare_amount <=50").collect()
newdf4 = sc.parallelize([Row(amount_range="30,50",num_trips=new_row4[0].num_trips)]).toDF()
new_row5 = spark.sql("select sum(count) as num_trips from group_data where fare_amount > 50 and fare_amount <=100").collect()
newdf5 = sc.parallelize([Row(amount_range="50,100",num_trips=new_row5[0].num_trips)]).toDF()
new_row6 = spark.sql("select sum(count) as num_trips from group_data where fare_amount > 100").collect()
newdf6 = sc.parallelize([Row(amount_range=">100",num_trips=new_row6[0].num_trips)]).toDF()
finaldf = newdf1.union(newdf2).union(newdf3).union(newdf4).union(newdf5).union(newdf6)
finaldf.select(format_string('%s,%d', finaldf.amount_range, finaldf.num_trips)).write.save("task2a-sql.out", format="text")

