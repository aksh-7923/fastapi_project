import json
from read_json import read_json
from confluent_kafka import Consumer, KafkaError


def create_consumer(bootstrap_servers):

    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 63,
        'auto.offset.reset': 'earliest'
    }
    return Consumer(consumer_config)

def update(payload):
    old_employee = payload["old_employee"]
    new_employee = payload["new_employee"]

    with open("output.json", "r") as file:
        records = json.load(file)
    for i in range(len(records)):
        if records[i]==old_employee:
            records[i] = new_employee

            with open("output.json", "w") as file:
                json.dump(records, file)
                print("record updated!!")

            break
    else:
        print("Old_employee records doesn't exist!!")


def read_msgs(consumer, topic):
    # print(1)
    consumer.subscribe([topic])

    # Start consuming messages
    while True:
        # print(2)
        msg = consumer.poll(5.0)  # Specify a timeout for polling (in seconds)

        if msg is None:
            # print(3)
            continue
        if msg.error():
            # Mark partition as exhausted
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        payload = json.loads(msg.value().decode('utf-8'))
        # print(type(payload))
        print(f"Received message: {payload}")
        update(payload)


# Define the Kafka bootstrap servers
bootstrap_servers = 'localhost:9092'

try:
    # creating consumer
    put_consumer = create_consumer(bootstrap_servers)

    # consuming and updating the json file...
    read_msgs(put_consumer, "put")
finally:
    #consumer1.commit()
    # Close the consumer when done
    put_consumer.close()