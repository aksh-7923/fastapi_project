from confluent_kafka import Producer


def create_producer(bootstrap_server, client_id):

    """
    bootstrap_servers: str -- address of kafka server
    client_id: str -- unique client-id for the producer

    returns: a producer
    """

    producer_config = {
        'bootstrap.servers': bootstrap_server,
        'client.id': client_id
    }

    return Producer(producer_config)


# creating producer
bootstrap_server = 'localhost:9092'
producer1 = create_producer(bootstrap_server, 'python-producer')

producer1.produce("topic1", "Hello kafka3 !!")

#  Wait for any outstanding messages to be delivered and delivery reports received
producer1.flush()
