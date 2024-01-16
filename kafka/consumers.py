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
                Utility().add_record("output.json", msg)
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
                Utility().update_record("output.json", msg)


    def delete_operation(self, topic):
        self.consumer.subscribe([topic])
        while True:
            msg = self.read_msgs()
            if msg:
                Utility().delete_record("output.json", msg)

def callback(company_name, operation, future):
    print("Task completed for company '{}', operation '{}'. Result: {}".format(company_name, operation, future.result()))

def parameter_consumer(parameter_queue):

    global executor  # Declare executor as global
    global executor_lock
    global parameter_queue_lock
    while True:
        with parameter_queue_lock:
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
        
        # Acquire lock before accessing the shared executor
        with executor_lock:
            # Submit task to the thread pool with the callback function
            if executor:
                future = executor.submit(task_function)
                print(f" executing {operation} operation for {company_name}")
                future.add_done_callback(lambda f, c=company_name, op=operation: callback(c, op, f))

        time.sleep(5)


def pool_manager(parameter_queue, max_pools):

    global executor
    global executor_lock
    global parameter_queue_lock
    while True:
        # Check the number of tasks in the queue
        with parameter_queue_lock:
            num_tasks = parameter_queue.qsize()
            print(num_tasks)

        if num_tasks > 0:
            if not executor :
                # Create a new pool if there are tasks and either no pool exists or the existing pool is empty
                with executor_lock:
                    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_pools)
                    print("new executror created")
        elif num_tasks == 0 and executor:
            # Shutdown the pool if there are no tasks and the pool exists
            with executor_lock:
                executor.shutdown()
                print("removing extra executor")
                executor = None

        # Sleep for a short duration before checking again
        time.sleep(20)


if __name__ == "__main__":
    # my_instance = MyClass()
    # Use ThreadPoolExecutor with a maximum of 3 concurrent threads
    # with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:

    parameter_queue = queue.Queue()
    global executor
    global executor_lock
    global parameter_queue_lock
    # executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)  # Initialize executor with max_workers
    executor = None
    executor_lock = threading.Lock()  # Lock to protect the executor
    parameter_queue_lock = threading.Lock()
    max_pools = 3  # Maximum number of pools

    initial_company_name = input("Enter the company name (or 'exit' to quit): ")
    initial_operation = input("Enter the operation ('get', 'post', 'put', 'delete'): ")

    parameter_queue.put((initial_company_name, initial_operation))

    # Start the pool manager thread
    pool_manager_thread = threading.Thread(target=pool_manager, args=(parameter_queue, max_pools))
    pool_manager_thread.start()

    # Start the parameter consumer thread after adding an initial parameter to the queue
    parameter_consumer_thread = threading.Thread(target=parameter_consumer, args=(parameter_queue, ))
    parameter_consumer_thread.start()

    while True:
        time.sleep(10)
        with parameter_queue_lock:
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
            
        time.sleep(15)
        


    # Signal the parameter consumer thread to exit
    parameter_queue.put(('exit', 'exit'))

    # Wait for the parameter consumer thread to finish
    parameter_consumer_thread.join()
    # Wait for the pool manager thread to finish
    pool_manager_thread.join()

    print("Script has exited.")


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

