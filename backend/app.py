from flask import Flask
from flask_socketio import SocketIO
from backend.settings.config_dev import settings
from backend.socketio_api import create_socket

app = Flask(__name__,
            static_url_path='', 
            static_folder='../frontend')
app.config['SECRET_KEY'] = settings["FLASK_SECRET_KEY"]
socketio = SocketIO(app)

# serve the home page to all routes
@app.route("/")
def home_page():
    return app.send_static_file('index.html')

if __name__ == '__main__':
    socketio = create_socket(socketio)
    socketio.run(app)