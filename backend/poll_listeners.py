import threading
import pykafka
from datetime import datetime
import time
from settings.config_dev import settings

KAFKA_HOST = settings["KAFKA_HOST"]
KAFKA_PORT  = settings["KAFKA_PORT"]

kafkaClient = pykafka.KafkaClient(f"{KAFKA_HOST}:{KAFKA_PORT}")

# kafka listener that polls kafka message and trigger the callback function when a new message has arrived
def register_kafka_listener(topic, listener):
    def poll():
        topicClient = kafkaClient.topics[topic]
        while True:
            consumer = topicClient.get_simple_consumer()
            for message in consumer:
                # send messages to the listener
                if message is not None:
                    print(message.value)
                    listener(message.value)
    # run pool() in a daemon thread
    t = threading.Thread(target=poll, daemon=True)
    t.start()
    print("kafka poll has been started")

def register_time_listener(interval, listener):
    def poll():
        while True:
            current_time = datetime.now().strftime("%H:%M:%S")
            listener(current_time)
            time.sleep(interval)
    # run pool() in a daemon thread
    t = threading.Thread(target=poll, daemon=True)
    t.start()
    print("timer poll has been started")

    