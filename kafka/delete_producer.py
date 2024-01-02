import json
from confluent_kafka import Producer

def delete_producer(client_id, record):
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': client_id
                          }
    producer = Producer(producer_config)
    producer.produce("delete", json.dumps(record).encode("utf-8"))
    producer.flush()

record = {'FirstName': 'Roberto', 'LastName': 'Wright', 'Gender': 'M', 'DateOfBirth': '1993-04-28', 
        'Email': 'robertowright@gmail.com', 'Position': 'IT sales professional'}

delete_producer("delete_producer", record)