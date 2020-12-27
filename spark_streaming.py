from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils

# create a spark context
sc = SparkSession.builder\
        .appName("twitter-trend-streaming")\
        .master("local[2]")\
        .getOrCreate()

ssc = StreamingContext(sc, 2)

kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'twitter-stream', {'twitter':1})

kafkaStream.writeStream\
           .format("console")

ssc.start()
ssc.awaitTermination()