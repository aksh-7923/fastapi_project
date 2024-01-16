# script containing threads class for creating and assigning threads

import time
import threading
import concurrent.futures
from consumers import Consumers

class Threads():


    def __init__(self):
        self.executor_dict = {}


    def callback(self, company_name, operation, future, executor_id):
        print("Task completed for company '{}', operation '{}'. Result: {}".
                format(company_name, operation, future.result()))
        self.executor_dict[executor_id][1].pop(1)

    
    def create_executor(self, executor_id):
        self.executor_dict[executor_id] = [concurrent.futures.ThreadPoolExecutor(max_workers=3)]
        self.executor_dict[executor_id].append([])
        # self.executor_dict[executor_id] = {"thread": concurrent.futures.ThreadPoolExecutor(max_workers=3), "activ": 0}


    def assign_threads(self, company_name, operation, executor_id):

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

        # Submit task to the thread pool with the callback function
        future = self.executor_dict[executor_id][0].submit(task_function)
        self.executor_dict[executor_id][1].append(1)
        future.add_done_callback(lambda f, c=company_name, op=operation, id=executor_id: self.callback(c, op, f, id))


    def create_threads(self):

        while True:
            company_name = input("Enter the company name (or 'exit' to quit): ")
            if company_name.lower() == 'exit':
                break

            operation = input("Enter the operation ('get', 'post', 'put', 'delete'): ")
            if operation.lower() == 'exit':
                break

    
            for id, executor in self.executor_dict.items():
                if sum(executor[1]) <3:
                    self.assign_threads(company_name, operation, id)
                    break
            else:
                executor_id = f'executor-{int(time.time())}'
                self.create_executor(executor_id)
                self.assign_threads(company_name, operation, executor_id)

            time.sleep(10)


if __name__ == "__main__":
    Threads().create_threads()






        

        


        