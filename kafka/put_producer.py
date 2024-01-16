import json
from confluent_kafka import Producer

def put_producer(client_id, old_employee, new_employee):
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': client_id
                          }
    producer = Producer(producer_config)

    payload = {"old_employee": old_employee, "new_employee": new_employee}
    producer.produce("put", json.dumps(payload).encode('utf-8'))
    producer.flush()

old = {
        "Gender":"F"
    }
new = {
        "Gender":"F"
    }

put_producer("put_producer", old, new)