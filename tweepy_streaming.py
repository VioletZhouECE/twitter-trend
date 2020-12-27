from settings.config_dev import settings
import tweepy
import time 
import pykafka

CUSTOMER_KEY = settings["CUSTOMER_KEY"]
CUSTOMER_SECRET = settings["CUSTOMER_SECRET"]
ACCESS_TOKEN = settings["ACCESS_TOKEN"]
ACCESS_TOKEN_SECRET = settings["ACCESS_TOKEN_SECRET"]

# default host and port for kafka server 
HOST = "localhost"
PORT  = 9092

auth = tweepy.OAuthHandler(CUSTOMER_KEY, CUSTOMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

api = tweepy.API(auth)

class StreamListener(tweepy.StreamListener):
    def __init__(self):
        self.client = pykafka.KafkaClient(f"{HOST}:{PORT}")
        self.producer = self.client.topics[bytes("twitter-stream", "utf-8")].get_producer()

    def on_data(self, data):
        # publish to kafka topic 
        # pykafka producer only accepts a byte object
        self.producer.produce(bytes(data, encoding='utf-8'))
        return True

    def on_status(self, status):
        print(status.text)
        return False

streamListener = StreamListener()
twitterStream = tweepy.Stream(auth=api.auth, listener=streamListener)

twitterStream.sample()