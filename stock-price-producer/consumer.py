import json

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "TP_PRICES", bootstrap_servers=["localhost:29092"], api_version=(0, 10, 11)
)


for message in consumer:
    print(message.value)
