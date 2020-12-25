from settings.config_dev import settings
import tweepy

CUSTOMER_KEY = settings["CUSTOMER_KEY"]
CUSTOMER_SECRET = settings["CUSTOMER_SECRET"]
ACCESS_TOKEN = settings["ACCESS_TOKEN"]
ACCESS_TOKEN_SECRET = settings["ACCESS_TOKEN_SECRET"]

auth = tweepy.OAuthHandler(CUSTOMER_KEY, CUSTOMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

api = tweepy.API(auth)
for tweet in tweepy.Cursor(api.search, q='tweepy').items(10):
    print(tweet.text)