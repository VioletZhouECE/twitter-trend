from settings.config_dev import settings
import tweepy
import time 

CUSTOMER_KEY = settings["CUSTOMER_KEY"]
CUSTOMER_SECRET = settings["CUSTOMER_SECRET"]
ACCESS_TOKEN = settings["ACCESS_TOKEN"]
ACCESS_TOKEN_SECRET = settings["ACCESS_TOKEN_SECRET"]

auth = tweepy.OAuthHandler(CUSTOMER_KEY, CUSTOMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

api = tweepy.API(auth)

class StreamListener(tweepy.StreamListener):
    def on_data(self, data):
	    print(data)
	    return True

    def on_status(self, status):
        print(status.text)
        return False

streamListener = StreamListener()
twitterStream = tweepy.Stream(auth=api.auth, listener=streamListener)

#test: stream for 10s
twitterStream.sample()
time.sleep(10)
twitterStream.disconnect()