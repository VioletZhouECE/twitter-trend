from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, json_tuple, concat, lit, regexp_replace, explode, split, when
from pyspark.sql.types import StringType, ArrayType
from settings.config_dev import settings

KAFKA_HOST = settings["KAFKA_HOST"]
KAFKA_PORT = settings["KAFKA_PORT"]

# create a spark context
sc = SparkSession.builder\
        .appName("twitter-trend-streaming")\
        .master("local[2]")\
        .getOrCreate()

# load the streaming dataframe
kafkaStream = sc \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", f"{KAFKA_HOST}:{KAFKA_PORT}") \
  .option("subscribe", "twitter-stream") \
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

#filter all the rows here hashtag is null
filteredHashtagsStream = hashtagsStream.where(col("hashtag").isNotNull())

ds = filteredHashtagsStream \
  .writeStream \
  .format("console") \
  .start()

ds.awaitTermination()