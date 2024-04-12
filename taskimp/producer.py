from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, DoubleType,FloatType, StructField, DateType,StringType


spark = SparkSession.builder.appName("CSV to Kafka").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#Schema of the CSV file
schema = StructType([ 
    StructField("Date/Time", StringType(),True), 
    StructField("LV_ActivePower",DoubleType(),True), 
    StructField("Wind_Speed",DoubleType(),True), 
    StructField("Theoretical_Power_Curve", DoubleType(), True), 
    StructField("Wind_Direction", DoubleType(), True) 
  ])


# 1. Read this CSV with headers using spark.
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("/home/xs438-nikjad/Desktop/kafkaspark/dataset/final_data.csv")\
    
df.tail(10)    

# 2. Publish these records into Kafka in streaming fashion.
query = df.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "task1") \
        .save()
     

