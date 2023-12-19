from confluent_kafka import Producer

# Define the Kafka bootstrap servers
bootstrap_servers = 'your_kafka_bootstrap_servers'

"""
# Define a Kafka producer configuration
producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'client.id': 'python-producer'
}

# Create a Kafka producer instance
producer = Producer(producer_config)

# Define the Kafka topic to which you want to send messages
topic = 'your_kafka_topic'

# Example message to send
message = 'Hello, Kafka!'

# Produce a message to the topic
producer.produce(topic, value=message)

"""

class Producer:

    """
        creates a producer
    """

    def create_producer(bootstrap_servers, client_id):

        """
        bootstrap_servers: str -- address of kafka server
        client_id: str -- unique client-id for the producer

        returns: an instance producer
        """

        producer_config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': client_id
        }

        return Producer(producer_config)


    #sending messages
    def send_msg(self, topic, message):

        """ 
        sends messages to the internal buffer
        params: topic: str -- name of topic where message is to be sent
                message: str -- message to be sent
        """ 

        self.produce(topic, value=message)

#creating producer
producer1 = Producer.create_producer(bootstrap_servers, 'python-producer')

#producer1.send_msg()

#  Wait for any outstanding messages to be delivered and delivery reports received
producer.flush()
