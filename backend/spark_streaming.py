from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, json_tuple, concat, lit, regexp_replace, explode, split, when, desc, window, current_timestamp, unix_timestamp, split
from pyspark.sql.types import StringType, ArrayType, TimestampType, IntegerType
from settings.config_dev import settings

KAFKA_HOST = settings["KAFKA_HOST"]
KAFKA_PORT = settings["KAFKA_PORT"]

# create a spark session
spark = SparkSession.builder\
        .appName("twitter-trend-streaming")\
        .master("local[*]")\
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# load the streaming dataframe
kafkaStream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:{KAFKA_PORT}") \
  .option("subscribe", "twitter-stream-input") \
  .option("failOnDataLoss", "false")\
  .load()

# parse value column to json and extract id and entities columns 
kafkaStream = kafkaStream.withColumn("value", col("value").cast(StringType()))\
                         .withColumn("value", regexp_replace(col("value"), r"\"source\".*\"truncated\"", "\"truncated\""))\
                         .select(json_tuple(col("value"), "created_at", "id", "entities"))\
                         .withColumnRenamed("c0", "created_at")\
                         .withColumnRenamed("c1", "id")\
                         .withColumnRenamed("c2", "entities")

# filter the null rows (those are the deleted tweets)
filteredKafkaStream = kafkaStream.where(col("id").isNotNull())

# extract hashtags from entities and explode arrays
hashtagsStream = filteredKafkaStream.withColumn("hashtags", json_tuple(col("entities"), "hashtags"))\
                                    .withColumn("hashtags", regexp_replace(col("hashtags"), r"^\[", ""))\
                                    .withColumn("hashtags", regexp_replace(col("hashtags"), r"\]$", ""))\
                                    .withColumn("hashtags", split(col("hashtags"), "},"))\
                                    .withColumn("hashtag", explode("hashtags"))\
                                    .withColumn("hashtag", regexp_replace(col("hashtag"), r"^(\{\"text\"):(.*)(,\"indices\".*)", "$2"))\
                                    .withColumn("hashtag", when(col("hashtag") == "", None).otherwise(col("hashtag")))\
                                    .drop("hashtags")

# filter all the rows here hashtag is null
filteredHashtagsStream = hashtagsStream.where(col("hashtag").isNotNull())

# time = process_time 
filteredHashtagsStream = filteredHashtagsStream.withColumn("time", current_timestamp())

# count hashtags over a 10-minute period, update every minute
hashtagWindowAgg = filteredHashtagsStream\
             .groupBy(window("time", "10 minutes", slideDuration="1 minute"), col("hashtag"))\
             .count()\
             .withColumn("window", col("window").cast(StringType()))

# select window that starts 9 mins ago, and pick the top 15 records 
hashtagAgg = hashtagWindowAgg.withColumn("start_time", regexp_replace(split("window", ",")[0], r"\[", ""))\
                             .withColumn("end_time", regexp_replace(split("window", ",")[1], r"\]", ""))\
                             .withColumn("time_diff", unix_timestamp() - unix_timestamp("start_time", format="yyyy-MM-dd HH:mm:ss"))\
                             .where((col("time_diff").cast(IntegerType())>540) & (col("time_diff").cast(IntegerType())<600))\
                             .orderBy(desc("count"))\
                             .limit(15)

# as required by kafka schema
# use &$% as a separator
kafkaAgg = hashtagAgg.withColumn("count", col("count").cast(StringType()))\
                     .withColumn("value", concat(col("start_time"), lit("&$%"), col("end_time"), lit("&$%"), col("hashtag"), lit("&$%"), col("count")))\
                     .select("start_time", "end_time", "value")

# output to kafka sink 
ds = kafkaAgg\
    .writeStream\
    .outputMode("complete")\
    .format("kafka")\
    .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:{KAFKA_PORT}")\
    .option("checkpointLocation", "checkpoint_twitter_app")\
    .option("topic", "twitter-stream-output")\
    .start()

ds.awaitTermination()

