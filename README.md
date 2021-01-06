# twitter-trend
Anaylze tweets returned by twitter sample stream API and display the most popular twitter hashtags in real time.

# Dataflow:
[twitter sample stream API](https://developer.twitter.com/en/docs/twitter-api/v1/tweets/sample-realtime/overview) -> kafka -> spark structured streaming -> kafka -> flask server (redis to cache the latest dashboard data)-> socketio -> frontend dashboard

# Demo:
[Spark Structured Streaming Output](/demo/spark_console_output.png)
<br>
<br>
![Real-time hashtag dashboard](/demo/hashtag_dashboard.png)


