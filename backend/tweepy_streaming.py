from backend.settings.config_dev import settings
import tweepy
import time 
import pykafka

CUSTOMER_KEY = settings["CUSTOMER_KEY"]
CUSTOMER_SECRET = settings["CUSTOMER_SECRET"]
ACCESS_TOKEN = settings["ACCESS_TOKEN"]
ACCESS_TOKEN_SECRET = settings["ACCESS_TOKEN_SECRET"]

KAFKA_HOST = settings["KAFKA_HOST"]
KAFKA_PORT  = settings["KAFKA_PORT"]

auth = tweepy.OAuthHandler(CUSTOMER_KEY, CUSTOMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

api = tweepy.API(auth)

class StreamListener(tweepy.StreamListener):
    def __init__(self):
        self.client = pykafka.KafkaClient(f"{KAFKA_HOST}:{KAFKA_PORT}")
        self.producer = self.client.topics[bytes("twitter-stream-input", "utf-8")].get_producer()

    def on_data(self, data):
        # publish to kafka topic 
        # pykafka producer only accepts byte objects
        self.producer.produce(bytes(data, encoding='utf-8'))
        return True

    def on_status(self, status):
        print(status.text)
        return False

streamListener = StreamListener()
twitterStream = tweepy.Stream(auth=api.auth, listener=streamListener)

twitterStream.sample()