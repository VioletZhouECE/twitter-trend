from app import socketio
from datetime import datetime
import threading

# test: send a random number every 5 seconds
def send_hashtag(socketio):
        threading.Timer(5.0, send_hashtag, args=[socketio]).start()
        current_time = datetime.now().strftime("%H:%M:%S")
        socketio.emit('hashtagData', current_time, namespace='/hashtag')
        return socketio

def create_socket(socketio):
    socketio = send_hashtag(socketio)
    return socketio


