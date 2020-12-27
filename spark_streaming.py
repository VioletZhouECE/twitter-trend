from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark import SparkContext

# default host and port for kafka server 
HOST = "localhost"
PORT  = 9092

# create a spark context
spark = SparkSession.builder\
        .appName("twitter-trend-streaming")\
        .master("local[2]")\
        .getOrCreate()

kafkaStream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "twitter-stream") \
  .load()

print(kafkaStream.collect())