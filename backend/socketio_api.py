from datetime import datetime
import threading
from poll_listeners import register_kafka_listener, register_time_listener

def create_socket(socketio):
    socketio = send_hashtag(socketio)
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


