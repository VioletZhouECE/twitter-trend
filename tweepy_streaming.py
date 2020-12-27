from settings.config_dev import settings
import tweepy
import time 
import redis

CUSTOMER_KEY = settings["CUSTOMER_KEY"]
CUSTOMER_SECRET = settings["CUSTOMER_SECRET"]
ACCESS_TOKEN = settings["ACCESS_TOKEN"]
ACCESS_TOKEN_SECRET = settings["ACCESS_TOKEN_SECRET"]

HOST = "localhost"
PORT  = 6379

auth = tweepy.OAuthHandler(CUSTOMER_KEY, CUSTOMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

api = tweepy.API(auth)

redis = redis.Redis(host=HOST, port=PORT)

class StreamListener(tweepy.StreamListener):
    def on_data(self, data):
        # publish data to localhost, port 6379
	    redis.publish('twitter-data-stream', data)
	    return True

    def on_status(self, status):
        print(status.text)
        return False

streamListener = StreamListener()
twitterStream = tweepy.Stream(auth=api.auth, listener=streamListener)

#test: stream for 10s
twitterStream.sample()