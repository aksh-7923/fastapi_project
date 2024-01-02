import json
import requests
from confluent_kafka import Producer


class Producers:

    def __init__(self, bootstrap_server, client_id):
        producer_config = {
        'bootstrap.servers': bootstrap_server,
        'client.id': client_id
                          }

        self.producer = Producer(producer_config)

    def get_producer(self, record):
        # url = ""
        res = requests.get("http://localhost:8000/emp/read", params = record)
        results = res.json()
        print(results)
        for result in results:
            self.producer.produce("get", json.dumps(result).encode('utf-8'))
        self.producer.flush()

test = Producers('localhost:9092', "put_producer1")
test.get_producer({"Gender": "'F'"})

# if __name__=="__main__":
#     # creating producer
#     bootstrap_server = 'localhost:9092'
#     producer1 = create_producer(bootstrap_server, 'python-producer')

#     producer1.produce("topic1", "Hello kafka3 !!")

#     #  Wait for any outstanding messages to be delivered and delivery reports received
#     producer1.flush()

# def create_producer(bootstrap_server, client_id):

#     """
#     bootstrap_servers: str -- address of kafka server
#     client_id: str -- unique client-id for the producer

#     returns: a producer
#     """

#     producer_config = {
#         'bootstrap.servers': bootstrap_server,
#         'client.id': client_id
#     }

#     return Producer(producer_config)

# get_producer = create_producer(bootstrap_server, "get_producer")

# def send_to_kafka(producer, topic, message):
#     producer.produce(topic, message)
#     producer.flush()

# send_to_kafka(producer, "get", json.dumps(response).encode('utf-8'))
