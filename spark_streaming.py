from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# create a spark context
sc = SparkSession.builder\
        .appName("twitter-trend-streaming")\
        .master("local[2]")\
        .getOrCreate()

kafkaStream = sc \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "twitter-stream") \
  .load()

# decode value colume(utf-8)
kafkaStream = kafkaStream.withColumn("value", col("value").cast(StringType()))

ds = kafkaStream \
  .writeStream \
  .format("console") \
  .start()

ds.awaitTermination()