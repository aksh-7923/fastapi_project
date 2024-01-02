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

old = {'FirstName': 'Roberto', 'LastName': 'Wright', 'Gender': 'F', 'DateOfBirth': '1993-04-28', 
        'Email': 'robertowright@gmail.com', 'Position': 'IT sales professional'}
new = {'FirstName': 'Roberto', 'LastName': 'Wright', 'Gender': 'M', 'DateOfBirth': '1993-04-28', 
        'Email': 'robertowright@gmail.com', 'Position': 'IT sales professional'}

put_producer("put_producer", old, new)