from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import sys
import requests

# redis HOST and PORT
HOST = "localhost"
PORT  = 6379

# create a spark context
spark = SparkSession.builder\
        .appName("twitter-trend-streaming")\
        .master("local[2]")\
        .config("spark.redis.host", HOST)\
        .config("spark.redis.port", PORT)\
        .getOrCreate()

# # create the Streaming Context from the above spark context with interval size 2 seconds
# ssc = StreamingContext(spark, 2)
# # setting a checkpoint to allow RDD recovery
# ssc.checkpoint("checkpoint_twitterApp")
# read data from redis
dataStream = spark.readStream\
                .format("org.apache.spark.sql.redis")\
                .option("stream.keys", "twitter-data-stream")\
                .load()

print(dataStream.collect())