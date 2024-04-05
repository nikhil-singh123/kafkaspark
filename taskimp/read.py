
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json ,col
from pyspark.sql.types import *
 
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
        .option("subscribe", "topic2") \
        .option("startingOffsets" , "earliest")\
        .load()
#kafka_data.show()
kafka_data.printSchema()
df1 = kafka_data.selectExpr("cast(value as string) as value")
json_expanded_df = df1.withColumn("value", from_json(df1["value"], schema)).select("value.*")
 
# df1= kafka_data.selectExpr("cast(value as string) as value")    
# json_expanded_df = df1.select(from_json(col("value"), schema).alias("parsed_data"))
# json_expanded_df.show(truncate=False)
json_expanded_df.printSchema()
json_expanded_df.writeStream.format("console").outputMode("append").start().awaitTermination()