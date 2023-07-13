import json
from kafka import KafkaConsumer

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers=["localhost:29092"],
    api_version=(0, 10, 11),
)

emails_sent = set()

print("Iniciando...")

while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        customer_email = consumed_message["customer_email"]
        print(f"Enviando email para {customer_email}")

        emails_sent.add(customer_email)

        print(f"Emails enviados para {len(emails_sent)} emails unicos")
