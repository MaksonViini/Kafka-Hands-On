import json
import time
from concurrent.futures import ThreadPoolExecutor
import random

from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 50

producer = KafkaProducer(bootstrap_servers=["localhost:29092"], api_version=(0, 10, 11))


def send_order(i):
    itens = ["Air Max", "Furadeira", "Meia", "Desodorante", "Chuveiro"]
    itens_price = [10, 15, 18, 20, 22]
    item_escolhido = random.choice(itens)

    data = {
        "order_id": i,
        "user_id": f"user_{i}",
        "total_cost": itens_price[itens.index(item_escolhido)],
        "items": item_escolhido,
    }
    producer.send(ORDER_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
    print(f"Enviado com sucesso item {i}")


with ThreadPoolExecutor() as executor:
    futures = []
    for i in range(1, ORDER_LIMIT):
        futures.append(executor.submit(send_order, i))

    # Aguarda a conclusão de todas as tarefas
    for future in futures:
        future.result()
