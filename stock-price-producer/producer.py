import json
from time import sleep

from dotenv import load_dotenv
from kafka import KafkaProducer
from yahooquery import Ticker

from utils import delivery_report

load_dotenv()


producer = KafkaProducer(bootstrap_servers=["localhost:29092"], api_version=(0, 10, 11))


symbols = ["AAPL", "ITSA4.SA", "NVDA", "META", "NFLX"]

tickers = Ticker(symbols=symbols)

prices = tickers.price

message = {"prices": {}}

for i in range(5):
    print(f"it {i}")

    for symbol in symbols:
        data = prices[symbol]

        price = data["regularMarketPrice"]

        print(f"The price of {symbol} is {price}")

        message["prices"][symbol] = price

    future = producer.send(
        "TP_PRICES", json.dumps(message).encode("utf-8")
    ).add_callback(delivery_report)
    print(f"Mensagem {i} enviada")

    sleep(1)

producer.flush()
