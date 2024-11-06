from kafka import KafkaConsumer, KafkaProducer
import json
import random
import time
import threading
from config import KAFKA_BROKER_URL, TASK_TOPIC, HEARTBEAT_TOPIC, WORKER_TOPIC
from queue import Queue

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda m: json.dumps(m).encode('ascii')
)

# Dictionary to keep track of worker status
workers = {}

# Queue to store pending tasks when no workers are available
task_queue = Queue()

def handle_heartbeats():
    consumer = KafkaConsumer(
        HEARTBEAT_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda m: json.loads(m.decode('ascii'))
    )
    
    for message in consumer:
        worker_id = message.value.get("worker_id")
        worker_status = message.value.get("status")
        current_time = time.time()
        
        # If worker ID is new, add to workers as available
        if worker_id not in workers:
            workers[worker_id] = {"status": worker_status, "last_heartbeat": current_time}
            print(f"Registered new worker {worker_id} as {worker_status}.")
        else:
            # Update heartbeat timestamp and status for existing worker
            workers[worker_id]["last_heartbeat"] = current_time
            workers[worker_id]["status"] = worker_status
            # print(f"Received heartbeat from worker {worker_id}. Status: {worker_status}")

def handle_tasks():
    consumer = KafkaConsumer(
        TASK_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda m: json.loads(m.decode('ascii'))
    )

    def try_assign_task():
        """
        Tries to assign any pending tasks to available workers.
        This function is called periodically within the task handling loop.
        """
        # Check for available workers
       
        
        # Assign tasks from the queue if workers are available
        while not task_queue.empty() :
            available_workers = [worker_id for worker_id, details in workers.items() if details["status"] == "available"]

            if available_workers : 
                task = task_queue.get()
                selected_worker = random.choice(available_workers)
                workers[selected_worker]["status"] = "busy"
                print(f"Assigned task {task['taskId']} to worker {selected_worker}")

                # Send the task to the WORKER_TOPIC with the worker ID
                task_message = {
                    "worker_id": selected_worker,
                    "task": task
                }
                producer.send(WORKER_TOPIC, task_message)
                available_workers.remove(selected_worker)  # Remove the selected worker from available pool
            else :
                continue

    for message in consumer:
        task = message.value
        print(f"Received task {task['taskId']}")

        # Add the task to the queue
        task_queue.put(task)

        # Attempt to assign pending tasks
        try_assign_task()



def __main__():
    # Run heartbeat and task handling in separate threads
    heartbeat_thread = threading.Thread(target=handle_heartbeats)
    task_thread = threading.Thread(target=handle_tasks)
    
    heartbeat_thread.start()
    task_thread.start()

    # Join threads to wait for them to finish (if desired)
    heartbeat_thread.join()
    task_thread.join()

if __name__ == "__main__":
    __main__()
