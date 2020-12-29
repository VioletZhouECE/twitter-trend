import os
from dotenv import load_dotenv
load_dotenv()

settings = {
    "CUSTOMER_KEY": os.getenv("CUSTOMER_KEY"),
    "CUSTOMER_SECRET": os.getenv("CUSTOMER_SECRET"),
    "ACCESS_TOKEN": os.getenv("ACCESS_TOKEN"),
    "ACCESS_TOKEN_SECRET": os.getenv("ACCESS_TOKEN_SECRET"),
    "KAFKA_HOST": "localhost",
    "KAFKA_PORT": 9092
}
