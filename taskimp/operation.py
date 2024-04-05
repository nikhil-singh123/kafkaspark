import pyspark
 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
 
 
# create sparksession
spark = SparkSession.builder \
    .appName("ReadFromKafkaTopic") \
    .getOrCreate()
 
 
# Define Kafka parameters
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "topic2"
 
# read data from kafka topic and put in dataframe
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", kafka_topic) \
  .option("startingOffsets", "earliest") \
  .load()
 
 
json_schema=StructType([
    StructField("Date/Time",StringType(),True),
    StructField("LV_ActivePower", DoubleType(), True),
    StructField("Wind_Speed", DoubleType(), True),
    StructField("Theoretical_Power_Curve", DoubleType(), True),
    StructField("Wind_Direction", DoubleType(), True),
 
    ])
 
 
json_df = df.selectExpr("cast(value as string) as value")
 
 
 
json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], json_schema)).select("value.*")
 
 
 
# writing on console
writing_df = json_expanded_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()
 
writing_df.awaitTermination()
 
 
