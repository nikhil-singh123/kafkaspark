
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
 
spark = SparkSession.builder \
    .appName("ReadDeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Delta table path
delta_table_path = "/home/xs438-nikjad/Desktop/kafkaspark/deltatable"
 
# Read Delta table as DataFrame
df = spark.read.format("delta").load(delta_table_path)
df_map=spark.read.format("delta").load(delta_table_path)
 
# Analysis Task - 1. Read the data from delta lake using spark
print("First question answer:-")
df.show(370)
 
 
#2. Calculate number of distinct `signal_ts` datapoints per day

df2 = df.select("signal_tc").distinct()
val =df2.count()
 
print("Second question answer is:")
 
print(val)

 
df2=df.groupBy("signal_date").count()
df2.show(400)
print(df2.count())
 
# 3.Calculate Average value of all the signals per hour
print("Third question answer:-")
print("average of lv activepower is \\\\\\\\\ ")
df_avg=  df.agg({"signals.LV_ActivePower":"avg",
                 "signals.Wind_Speed":"avg",
                 "signals.Theoretical_Power_Curve":"avg",
                 "signals.Wind_Direction":"avg"})
 
df_avg.show()
 
 
 
 
# # 4. Add a column in the dataframe of above named as `generation_indicator` and it will have
# value as
# a. if LV ActivePower<200KW then Low
# b. 200<= LV ActivePower < 600 then Medium
# c. 600< =LV ActivePower < 1000 then High
# d. 1000<=LV ActivePowe then Exceptional
 
df = df.withColumn("generation_indicator",when(col("signals.LV_ActivePower") < 200, "Low")\
    .when((col("signals.LV_ActivePower") >= 200) & (col("signals.LV_ActivePower") < 600), "Medium")\
    .when((col("signals.LV_ActivePower") >= 600) & (col("signals.LV_ActivePower") < 1000), "High")\
    .otherwise("Exceptional"))
 
print("Fourth question answer:-")
df.show()
 
 
 
# 5. Create a new dataframe with following json:
json_data_df = [
    {"sig_name": "LV_ActivePower", "sig_mapping_name": "LV_ActivePower_average"},
    {"sig_name": "Wind_Speed", "sig_mapping_name": "Wind_Speed_average"},
    {"sig_name": "Theoretical_Power_Curve", "sig_mapping_name": "Theoretical_Power_Curve_average"},
    {"sig_name": "Wind_Direction", "sig_mapping_name": "Wind Direction_average"}
]
  
# Create DataFrame from JSON data
new_df = spark.createDataFrame([Row(**x) for x in json_data_df])
 
print("Fifth question answer:-")
# Printing the dataframe schema
new_df.printSchema()
 
 
# 6th question
broadcast_df = df.join(broadcast(new_df),
                       df.generation_indicator == new_df.sig_name,
                       "left_outer")
# Updating the 'generation_indicator' column
broadcast_df = broadcast_df.withColumn("generation_indicator", broadcast_df.sig_mapping_name)
 
# Drop columns
broadcast_df = broadcast_df.drop("sig_name", "sig_mapping_name")
 
# Showing the updated DataFrame
broadcast_df.show()
 
 
 
 
 
 
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,org.apache.spark:spark-streaming-kafka_2.11:1.6.3,io.delta:delta-spark_2.12:3.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
