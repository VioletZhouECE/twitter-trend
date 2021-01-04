import threading
from kafka import KafkaConsumer
from datetime import datetime, timedelta
import time
import redis
from settings.config_dev import settings

# kafka listener that polls kafka message and trigger the callback function when a new message has arrived
def register_kafka_listener(topic, listener):
    def poll():
        # timeout is set to 10min for testing purpose
        TIMEOUT = 600000
        KAFKA_HOST = settings["KAFKA_HOST"]
        KAFKA_PORT  = settings["KAFKA_PORT"]
        consumer = KafkaConsumer('twitter-stream-output', bootstrap_servers=[f'{KAFKA_HOST}:{KAFKA_PORT}'], auto_offset_reset='latest')
        NUM_OF_MESSAGE = 20

        # buffer array to buffer messages
        buffer = []
        prevCount = 0

        #instantiate redis
        r = redis.Redis()
        
        # start the polling process
        consumer.poll(timeout_ms=TIMEOUT)
        for message in consumer:
            if message is not None:
                msg = message.value.decode("utf-8")
                hashtag = msg.split(":")[0]
                count = int(msg.split(":")[1])
                # meeting this condition means that we did not consume data from the beginning of the 20-message batch, so reset
                # to-do: find a better way to ensure that buffer starts from the 1 message (with the largest count)
                if count > prevCount and len(buffer) < NUM_OF_MESSAGE:
                    buffer = [msg]
                    prevCount = count 
                    continue
                buffer.append(msg)
                prevCount = count
                # if the buffer is full, send messages to the listener and store it to redis
                if len(buffer) == NUM_OF_MESSAGE:
                    r.setex("hashtag", timedelta(minutes=2), value=','.join(buffer))
                    print(r.get("hashtag").decode("utf-8"))
                    listener(buffer)
                    buffer = []

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

    