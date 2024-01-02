import json
from read_json import read_json
from confluent_kafka import Consumer

def create_consumer(bootstrap_servers):

    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 64,
        'auto.offset.reset': 'earliest'
    }
    return Consumer(consumer_config)

def delete_record(record):

    with open("output.json", "r") as file:
        data = json.load(file)

    for i in range(len(data)):
        if data[i]==record:
            data.pop(i)

            with open("output.json", "w") as file:
                json.dump(data, file)
                print("record deleted!!")

            break
    else:
        print("Record doesn't exist!!") 


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

        record = json.loads(msg.value().decode('utf-8'))
        print(record)
        delete_record(record)


# Define the Kafka bootstrap servers
bootstrap_servers = 'localhost:9092'

try:
    # creating consumer
    delete_consumer = create_consumer(bootstrap_servers)

    # consuming and updating the json file...
    read_msgs(delete_consumer, "delete")
finally:
    #consumer1.commit()
    # Close the consumer when done
    delete_consumer.close()
