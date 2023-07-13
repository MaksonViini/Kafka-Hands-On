import json

from kafka import KafkaConsumer, KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"


consumer = KafkaConsumer(
    ORDER_KAFKA_TOPIC, bootstrap_servers=["localhost:29092"], api_version=(0, 10, 11)
)

producer = KafkaProducer(bootstrap_servers=["localhost:29092"], api_version=(0, 10, 11))


print("Iniciando...")


while True:
    for message in consumer:
        print("Transacao em andamento...")
        consumed_message = json.loads(message.value.decode())
        print(consumed_message)

        user_id = consumed_message["user_id"]
        total_cost = consumed_message["total_cost"]

        data = {
            "customer_id": user_id,
            "customer_email": f"{user_id}@gmail.com",
            "total_cost": total_cost,
        }

        print("Transacao efetuada com sucesso!")

        producer.send(ORDER_CONFIRMED_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
