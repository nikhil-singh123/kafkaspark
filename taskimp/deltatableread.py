
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
 
# Path to the Delta table
delta_table_path = "/home/xs438-nikjad/Desktop/kafkaspark/deltatable2"
 
# Read Delta table as DataFrame
df = spark.read.format("delta").load(delta_table_path)
df_map=spark.read.format("delta").load(delta_table_path)
 
# Show the DataFrame
df.show(370)
 
 
#2 question
 
df2 = df.select("signal_tc").distinct()
val =df2.count()
 
# df2.show()
 
print("second question answer is:")
 
print(val)
 
 
df2=df.groupBy("signal_date").count()
df2.show(400)
print(df2.count())
 
# 3rd question
 
print("average of lv activepower is \\\\\\\\\ ")
df_avg=  df.agg({"signals.LV_ActivePower":"avg",
                 "signals.Wind_Speed":"avg",
                 "signals.Theoretical_Power_Curve":"avg",
                 "signals.Wind_Direction":"avg"})
 
 
 
 
df_avg.show()
 
 
 
 
# 4th questionnnn
 
df = df.withColumn("generation_indicator",when(col("signals.LV_ActivePower") < 200, "Low")\
    .when((col("signals.LV_ActivePower") >= 200) & (col("signals.LV_ActivePower") < 600), "Medium")\
    .when((col("signals.LV_ActivePower") >= 600) & (col("signals.LV_ActivePower") < 1000), "High")\
    .otherwise("Exceptional"))
 
 
df.show()
 
 
 
# 5th question
 
 
json_data_df = [
    {"sig_name": "LV_ActivePower", "sig_mapping_name": "LV_ActivePower_average"},
    {"sig_name": "Wind_Speed", "sig_mapping_name": "Wind_Speed_average"},
    {"sig_name": "Theoretical_Power_Curve", "sig_mapping_name": "Theoretical_Power_Curve_average"},
    {"sig_name": "Wind_Direction", "sig_mapping_name": "Wind Direction_average"}
]
 
# json_data_df =[
#     {""}
# ]
 
 
 
 
 
# Create DataFrame from JSON data
new_df = spark.createDataFrame([Row(**x) for x in json_data_df])
 
# Show the DataFrame
# new_df.show()
# json_data_df.printSchema()
 
new_df.printSchema()
 
 
 
 
# 6th question
 
# df.show()
broadcast_df = df.join(broadcast(new_df),
                       df.generation_indicator == new_df.sig_name,
                       "left_outer")
# Update the 'generation_indicator' column

# broadcast_df = broadcast_df.withColumn("generation_indicator", broadcast_df.sig_mapping_name)
 
# Drop columns
# broadcast_df = broadcast_df.drop("sig_name", "sig_mapping_name")
 
# Show the updated DataFrame
# broadcast_df.show()
 
 
 
 
 
 
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-streaming-kafka_2.11:1.6.3,io.delta:delta-spark_2.12:3.1.0
