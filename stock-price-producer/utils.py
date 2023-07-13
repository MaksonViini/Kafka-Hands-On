def delivery_report(err, msg):
    if err is not None:
        print(f"Mensagem falhou {err}")
    else:
        print(f"Mensagem entregue a {msg.topic} [{msg.partition()}]")
