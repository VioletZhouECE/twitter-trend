from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, json_tuple, concat, lit, regexp_replace, explode, split, when, desc
from pyspark.sql.types import StringType, ArrayType
from backend.settings.config_dev import settings

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

# count hashtags and orderBy count
hashtagAgg = filteredHashtagsStream.groupBy("hashtag").count().orderBy(desc("count")).limit(20)

# as required by kafka schema
kafkaAgg = hashtagAgg.withColumnRenamed("hashtag", "key")\
                     .withColumn("count", col("count").cast(StringType()))\
                     .withColumnRenamed("count", "value")

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