from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json ,col
from pyspark.sql.types import *
 
 # Subscriber Task- Read the data from Kafka in streaming fashion using spark
spark = SparkSession.builder \
        .appName("KafkaSubscriber") \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
schema = StructType() \
      .add("Date/Time",StringType(),True) \
      .add("LV_ActivePower",DoubleType(),True) \
      .add("Wind_Speed",DoubleType(),True) \
      .add("Theoretical_Power_Curve",DoubleType(),True) \
      .add("Wind_Direction",DoubleType(),True) \
      
kafka_data  = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "task1") \
        .option("startingOffsets" , "earliest")\
        .load()
#kafka_data.show()
kafka_data.printSchema()
df1 = kafka_data.selectExpr("cast(value as string) as value")
json_expanded_df = df1.withColumn("value", from_json(df1["value"], schema)).select("value.*")
 
json_expanded_df.printSchema()
json_expanded_df.writeStream.format("console").outputMode("append").start().awaitTermination()