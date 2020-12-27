from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, json_tuple, concat, lit, regexp_replace
from pyspark.sql.types import StringType
from settings.config_dev import settings

KAFKA_HOST = settings["KAFKA_HOST"]
KAFKA_PORT = settings["KAFKA_PORT"]

# create a spark context
sc = SparkSession.builder\
        .appName("twitter-trend-streaming")\
        .master("local[2]")\
        .getOrCreate()

kafkaStream = sc \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:{KAFKA_PORT}") \
  .option("subscribe", "twitter-stream") \
  .load()

# decode value column(utf-8)
kafkaStream = kafkaStream.withColumn("value", col("value").cast(StringType()))


# parse value column to json
# extract id and entities columns 
kafkaStream = kafkaStream.withColumn("value", regexp_replace(col("value"), r"\"source\".*\"truncated\"", "\"truncated\""))\
                         .select(json_tuple(col("value"), "created_at", "id", "entities"))\
                         .withColumnRenamed("c0", "created_at")\
                         .withColumnRenamed("c1", "id")\
                         .withColumnRenamed("c2", "entities")

ds = kafkaStream \
  .writeStream \
  .format("console") \
  .start()

ds.awaitTermination()