import json
import requests
from confluent_kafka import Producer


class Producers:

    def __init__(self, client_id):
        config = {
            'bootstrap.servers': 'localhost:9092',
            'client.id': client_id
        }

        self.producer = Producer(config)

    def get_producer(self, topic, condition):
        res = requests.get("http://localhost:8000/emp/read", params = condition)
        results = res.json()
        # print(results)
        for result in results:
            self.producer.produce(topic, json.dumps(result).encode('utf-8'))
        self.producer.flush()

    def put_producer(self, topic, old_employee, new_employee):
        payload = {"old_employee": old_employee, "new_employee": new_employee}
        self.producer.produce(topic, json.dumps(payload).encode('utf-8'))
        self.producer.flush()

    def delete_producer(self, topic, record):
        self.producer.produce("delete", json.dumps(record).encode("utf-8"))
        self.producer.flush()


if __name__=="__main__":
    
    condition = {"Gender": "'F'"}
    producer1 = Producers("producer1")

    old = {
        "Gender":"M"
    }
    new = {
        "Gender":"F"
    }

    old1 = {'FirstName': 'Jessica', 'LastName': 'Rice', 'Gender': 'M', 'DateOfBirth': '2008-08-24', 
            'Email': 'jessicarice@gmail.com', 'Position': 'Magazine journalist'}
    new1 = {'FirstName': 'Jessica', 'LastName': 'PRice', 'Gender': 'M', 'DateOfBirth': '2008-08-24', 
            'Email': 'jessicarice@gmail.com', 'Position': 'Magazine journalist'}



    delete_record1 = {
        "FirstName":"Roberto"
    }

    delete_record2 = {"FirstName": "Jessica", "LastName": "PRice", "Gender": "M", "DateOfBirth": "2008-08-24", 
                        "Email": "jessicarice@gmail.com", "Position": "Magazine journalist"}

    # producer1.get_producer("get", condition)
    # producer1.put_producer("put", old, new)
    producer1.delete_producer("delete", delete_record1)
