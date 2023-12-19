from confluent_kafka import Consumer, KafkaError

# Define the Kafka bootstrap servers
bootstrap_servers = 'bootstrap_servers'

"""
# Define a Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest' 
}

# Create a Kafka consumer instance
consumer = Consumer(consumer_config)
"""

class Consumer:
    
    def create_consumer(bootstrap_servers, group_id, offset):

        consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': offset
        } 
        return  

    def read_msgs(self, topic):
        self.subscribe([topic])

        # Start consuming messages
        while True:
            msg = self.poll(1.0)  # Specify a timeout for polling (in seconds)

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


# creating consumer
consumer1 = Consumer.create_consumer(bootstrap_servers, group_id, offset)

#consumer1.read_msgs()

# Close the consumer when done
consumer1.close()
