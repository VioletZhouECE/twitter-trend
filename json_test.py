from pyspark.sql import SparkSession
from pyspark.sql.functions import col, json_tuple, concat, lit, regexp_replace

# temporary test file 
data = [{"jstring": '''{"created_at":"Sun Dec 27 16:44:39 +0000 2020","id":1343236362749636612,"id_str":"1343236362749636612","text":"Baka may gustong humabol dyan bago mag bagong taon","source":"\u003ca href=\"https:\/\/mobile.twitter.com\" rel=\"nofollow\"\u003eTwitter Web App\u003c\/a\u003e","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":1036437165616455680,"id_str":"1036437165616455680","name":"\ud83c\udf7b drunken master \ud83c\udf7b","screen_name":"jrossanatalio","location":null,"url":null,"description":"eccedentesiast","translator_type":"none","protected":false,"verified":false,"followers_count":243,"friends_count":372,"listed_count":0,"favourites_count":4444,"statuses_count":2575,"created_at":"Mon Sep 03 02:14:11 +0000 2018","utc_offset":null,"time_zone":null,"geo_enabled":false,"lang":null,"contributors_enabled":false,"is_translator":false,"profile_background_color":"F5F8FA","profile_background_image_url":"","profile_background_image_url_https":"","profile_background_tile":false,"profile_link_color":"1DA1F2","profile_sidebar_border_color":"C0DEED","profile_sidebar_fill_color":"DDEEF6","profile_text_color":"333333","profile_use_background_image":true,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/1340296447334313986\/uFNMlvDK_normal.jpg","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/1340296447334313986\/uFNMlvDK_normal.jpg","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/1036437165616455680\/1599516283","default_profile":true,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":null,"coordinates":null,"place":null,"contributors":null,"is_quote_status":false,"quote_count":0,"reply_count":0,"retweet_count":0,"favorite_count":0,"entities":{"hashtags":[],"urls":[],"user_mentions":[],"symbols":[]},"favorited":false,"retweeted":false,"filter_level":"low","lang":"tl","timestamp_ms":"1609087479661"}'''}]

# create a spark context
sc = SparkSession.builder\
        .appName("json-test")\
        .master("local[2]")\
        .getOrCreate()

df = sc.createDataFrame(data, ["jstring"])

df = df.withColumn("jstring", regexp_replace("jstring", r"\"source\".*\"truncated\"", "\"truncated\""))

#print(df.first())

df = df.select(json_tuple("jstring", "created_at", "id"))

print(df.show())