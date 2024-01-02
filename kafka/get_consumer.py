import json
from confluent_kafka import Consumer, KafkaError


def create_consumer(bootstrap_servers):

    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 62,
        'auto.offset.reset': 'earliest'
    }
    return Consumer(consumer_config)

def write_json(record):
    try:
        with open("output.json", "r") as json_file:
            existing_data = json.load(json_file)
            # print(existing_data)
    except (FileNotFoundError, json.decoder.JSONDecodeError):
        # If the file doesn't exist, start with an empty dictionary
        existing_data = []

    # Update the existing data with the new results
    print(record)
    # print(type(record))
    existing_data.append(record)
    # print(existing_data)

    # Write the updated data to the JSON file
    with open("output.json", "w") as json_file:
        json.dump(existing_data, json_file)


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

        # Process the received message
        print(f"Received message: {msg.value().decode('utf-8')}")
        write_json(json.loads(msg.value().decode('utf-8')))


# if we want to append data one by one then we use this otherwise append records into list and dump everything together
        # with open(file_path, "a") as json_file:
        #     json_file.write(json.dumps(msg.value().decode('utf-8')) + "\n")
        #     json_file.write("\n")


# Define the Kafka bootstrap servers
bootstrap_servers = 'localhost:9092'

try:
    # creating consumer
    consumer1 = create_consumer(bootstrap_servers)

    # consuming and updating the json file...
    read_msgs(consumer1, "get")
finally:
    consumer1.commit()
    # Close the consumer when done
    consumer1.close()

"""
with open(file_path, "a") as json_file:
    json_file.write(json.dumps(msg.value().decode('utf-8')) + "\n")
    json_file.write("\n")

msgs.append(msg.value().decode('utf-8'))
print(f"Received message: {msg.value().decode('utf-8')}")

# Keep track of exhausted partitions
exhausted_partitions = set()
msgs = []

with open("output.json", "w") as json_file:
    data = [msg.value().decode('utf-8') for msg in msgs]
    json.dump(data, json_file)
"""
