from datetime import datetime
import threading
import redis
from poll_listeners import register_kafka_listener, register_time_listener

def create_socket(socketio):
    socketio = send_hashtag(socketio)
    socketio = getInitHashtagData(socketio)
    return socketio

def send_hashtag(socketio):
    def send_hashtag_listener(data):
        socketio.emit('hashtagData', data, namespace='/hashtag')
    register_kafka_listener("twitter-stream-output", send_hashtag_listener)
    return socketio

def send_time(socketio):
    def send_time_listener(data):
        socketio.emit('hashtagData', data, namespace='/hashtag')
    register_time_listener(1, send_time_listener)
    return socketio

# get the initial map data from redis storage
def getInitHashtagData(socketio):
    def handler():
        # get data from the redis store
        # to-do: investigate if transaction and watch is necessary here 
        r = redis.Redis()
        data = r.get("hashtag").decode("utf-8").split(",")
        if data is None:
            data = "no data available"
        socketio.emit('hashtagData', data, namespace='/hashtag')
    socketio.on_event('getInitData', handler, namespace='/hashtag')
    return socketio

