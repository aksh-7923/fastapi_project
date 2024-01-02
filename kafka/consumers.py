import time
import uuid
import json
import queue
import threading
import concurrent.futures
from utils import Utility
from confluent_kafka import Consumer


class Consumers():

    def __init__(self, group_id):

        client_id = f'{group_id}-{int(time.time())}-{uuid.uuid4()}'
        # print(client_id)
        consumer_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': group_id,
            'client.id': client_id,
            'auto.offset.reset': 'earliest'
            # enable.auto.commit = False
        }
        self.consumer =  Consumer(consumer_config)

    def read_msgs(self):
        msg = self.consumer.poll(5.0)  # Specify a timeout for polling (in seconds)

        if msg is None:
            return 
        if msg.error():
            # Mark partition as exhausted
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return
            else:
                print(f"Error: {msg.error()}")
                return {}

        message = json.loads(msg.value().decode('utf-8'))
        return message


    def get_operation(self, topic):
        self.consumer.subscribe([topic])

        # Start consuming messages
        while True:
        #for _ in range(10):
            msg = self.read_msgs()

            if msg:
                Utility().write_json("output.json", msg)
            elif msg is None:
                continue
            else:
                print(msg)
                break

    
    def put_operation(self, topic):
        self.consumer.subscribe([topic])

        # Start consuming messages
        while True:
            msg = self.read_msgs()
            if msg:
                Utility().update("output.json", msg)


    def delete_operation(self, topic):
        self.consumer.subscribe([topic])
        while True:
            msg = self.read_msgs()
            if msg:
                Utility().delete_record("output.json", msg)

def callback(company_name, operation, future):
    print("Task completed for company '{}', operation '{}'. Result: {}".format(company_name, operation, future.result()))

def parameter_consumer(parameter_queue, executor):

    while True:
        param = parameter_queue.get()  # Wait for a new parameter
        if param[0].lower() == 'exit' or param[1].lower() == 'exit':
            break

        company_name, operation = param

        # Create a new instance of Consumers for each task with the given company_name
        my_instance = Consumers(company_name)

        # Determine which operation to perform based on user input
        if operation == 'get':
            task_function = lambda: my_instance.get_operation("get")
        elif operation == 'put':
            task_function = lambda: my_instance.put_operation("put")
        elif operation == 'delete':
            task_function = lambda: my_instance.delete_operation("delete")
        else:
            print("Invalid operation. Please enter 'get', 'put', or 'delete'.")
            continue

        # Submit task to the thread pool with the callback function
        future = executor.submit(task_function)
        future.add_done_callback(lambda f, c=company_name, op=operation: callback(c, op, f))


if __name__ == "__main__":
    # my_instance = MyClass()

    # Use ThreadPoolExecutor with a maximum of 3 concurrent threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        parameter_queue = queue.Queue()

        initial_company_name = input("Enter the company name (or 'exit' to quit): ")
        initial_operation = input("Enter the operation ('get', 'post', 'put', 'delete'): ")
        # Start the parameter consumer thread after adding an initial parameter to the queue
        initial_param = (initial_company_name, initial_operation)
        parameter_queue.put(initial_param)

        # Start the parameter consumer thread
        parameter_consumer_thread = threading.Thread(target=parameter_consumer, args=(parameter_queue, executor))
        parameter_consumer_thread.start()

        while True:
            time.sleep(15)
            # Dynamically add new parameters to the queue
            company_name = input("Enter the company name (or 'exit' to quit): ")
            if company_name.lower() == 'exit':
                break

            operation = input("Enter the operation ('get', 'post', 'put', 'delete'): ")
            if operation.lower() == 'exit':
                break

            # Combine company_name and operation into a tuple and add to the queue
            new_param = (company_name, operation)
            parameter_queue.put(new_param)


        # Signal the parameter consumer thread to exit
        parameter_queue.put(('exit', 'exit'))

        # Wait for the parameter consumer thread to finish
        parameter_consumer_thread.join()

    print("Script has exited.")


    # def consumer_operation(self, topic, operation):
    #     self.consumer.subscribe([topic])

    #     # Start consuming messages
    #     while True:
    #         msg = self.read_msgs()

    #         if operation=="get":
    #             write_json(json.loads(msg.value().decode('utf-8')))
    #         elif operation=="put":
    #             update(msg)
    #         elif operation=="delete":
    #             delete_record(msg)
    #         else:
    #             print("Operation not allowed!!")
    #             break

# def callback(param, future):
#     print("Task completed for param '{}'. Result: {}".format(param, future.result()))

# def parameter_producer(parameter_queue):
#     # This function runs in a separate thread or process
#     while True:
#         # Dynamically add new parameters to the queue
#         new_param = input("Enter a new parameter (or 'exit' to quit): ")
#         if new_param.lower() == 'exit':
#             break
#         parameter_queue.put(new_param)




# if __name__=="__main__":
    # consumer1 = Consumers(77)
    # consumer2 = Consumers(78)
    # consumer3 = Consumers(79)

    # # Create threads with parameters for each function
    # thread1 = threading.Thread(target=consumer1.get_operation, args=("get",))
    # thread2 = threading.Thread(target=consumer2.put_operation, args=("put",))
    # thread3 = threading.Thread(target=consumer3.delete_operation, args=("delete",))

    # # Start the threads
    # thread1.start()
    # time.sleep(10)
    # thread2.start()
    # time.sleep(10)
    # thread3.start()

    # # Wait for all threads to finish
    # thread1.join()
    # thread2.join()
    # thread3.join()

