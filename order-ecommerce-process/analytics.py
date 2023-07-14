import json
from kafka import KafkaConsumer

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers=["localhost:29092"],
    api_version=(0, 10, 11),
)

orders_count = 0
total_revenue = 0

print("Iniciando...")

while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        total_cost = float(consumed_message["total_cost"])
        
        orders_count += 1
        total_revenue += total_cost

        print(f"Quantidade de pedidos hoje {orders_count}")
        print(f"Quantidade total de receita hoje {total_revenue}")
