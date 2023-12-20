from confluent_kafka import Consumer, KafkaError


def create_consumer(bootstrap_servers):

    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 1,
        'auto.offset.reset': 'earliest'
    }
    return Consumer(consumer_config)


def read_msgs(consumer, topic):
    consumer.subscribe([topic])

    # Start consuming messages
    while True:
        msg = consumer.poll(1.0)  # Specify a timeout for polling (in seconds)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event - not an error
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        # Process the received message
        print(f"Received message: {msg.value().decode('utf-8')}")


# Define the Kafka bootstrap servers
bootstrap_servers = 'localhost:9092'

# creating consumer
consumer1 = create_consumer(bootstrap_servers)

read_msgs(consumer1, "topic1")

# Close the consumer when done
consumer1.close()
