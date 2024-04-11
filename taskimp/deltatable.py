from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, to_timestamp, current_date, current_timestamp, date_format,struct,lit
from pyspark.sql.types import StructType, StructField, DoubleType, DateType, FloatType
from pyspark.sql.types import *
from pyspark.sql.functions import split
 

# Subscriber Task- Write this data received in delta format
# SparkSession
spark = SparkSession.builder \
    .appName("deltatable") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
 
# schema defining
myschema=StructType([
    StructField("Date/Time",StringType(),True),
    StructField("LV_ActivePower", DoubleType(), True),
    StructField("Wind_Speed", DoubleType(), True),
    StructField("Theoretical_Power_Curve", DoubleType(), True),
    StructField("Wind_Direction", DoubleType(), True),
 
    ])
 
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "task1"
checkpoint="/home/xs438-nikjad/Desktop/kafkaspark/checkpoint"
 
 
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
 
 
signals_map = {
    "LV_ActivePower": "LV_ActivePower",
    "Wind_Speed": "Wind_Speed",
    "Theoretical_Power_Curve": "Theoretical_Power_Curve",
    "Wind_Direction": "Wind_Direction"
}
 
 
update_df = json_expanded_df.withColumn('signal_date',to_date(split(json_expanded_df['Date/Time'],' ').getItem(0), ))\
    .withColumn('signal_tc',to_timestamp(json_expanded_df['Date/Time']))\
    .withColumn("create_date",date_format(current_date(), 'dd MM yyyy'))\
    .withColumn("create_ts",date_format(current_timestamp(), 'dd MM yyyy HH:mm:ss'))  \
    .withColumn("signals", struct([json_expanded_df[col].alias(col) for col in signals_map]))\
        .drop("Date/Time","LV_ActivePower","Wind_Speed","Theoretical_Power_Curve","Wind_Direction")
  
 
update_df.printSchema()
 
 
query = update_df.writeStream \
        .format("delta") \
        .outputMode("append")\
        .option("mergeSchema", "true")\
        .option("checkpointLocation", checkpoint)\
       .start("/home/xs438-nikjad/Desktop/kafkaspark/deltatable")\
    
query.awaitTermination()  
 
 
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-streaming-kafka_2.11:1.6.3,io.delta:delta-spark_2.12:3.1.0
                