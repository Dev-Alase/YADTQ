from kafka import KafkaConsumer, KafkaProducer
import json
import random
import time
from threading import Lock
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
worker_task_map = {}
WORKER_TIMEOUT = 15

# Locks for thread-safe access
workers_lock = Lock()
task_assignment_lock = Lock()
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

        with workers_lock:  # Acquire lock before modifying workers
            if worker_id not in workers:
                print(f"Registered new worker {worker_id} as {worker_status}.")
            else:
                print(f"Updated worker {worker_id} to status {worker_status}.")

            workers[worker_id] = {"status": worker_status, "last_heartbeat": current_time}

        # Initialize worker_task_map if not already present
        if worker_id not in worker_task_map:
            worker_task_map[worker_id] = []


def try_assign_task():
    """
    Runs indefinitely to assign tasks to available workers.
    """
    while True:
        with task_assignment_lock:  # Ensure only one thread executes this at a time
            if not task_queue.empty():
                with workers_lock:  # Acquire lock for thread-safe read
                    # Create a snapshot of available workers
                    available_workers = [
                        worker_id for worker_id, details in workers.items()
                        if details["status"] == "available"
                    ]

                if available_workers:
                    task = task_queue.get()
                    selected_worker = random.choice(available_workers)

                    with workers_lock:  # Update worker status inside the lock
                        workers[selected_worker]["status"] = "busy"

                    print(f"Assigned task {task['taskId']} to worker {selected_worker}")

                    # Update worker_task_map
                    worker_task_map[selected_worker] = []
                    worker_task_map[selected_worker].append(task)

                    # Send the task to the WORKER_TOPIC with the worker ID
                    task_message = {
                        "worker_id": selected_worker,
                        "task": task
                    }

                    try:
                        producer.send(WORKER_TOPIC, task_message)
                        producer.flush()
                        print("Successfully assigned task to the worker")
                    except Exception as e:
                        print(f"Error occurred while assigning task to worker: {e}")
                else:
                    # No available workers, wait briefly before retrying
                    time.sleep(0.1)
            else:
                # Task queue is empty, wait briefly before checking again
                time.sleep(0.1)


def check_inactive_workers():
    """
    Periodically checks for inactive workers in a separate thread.
    Removes them from the workers dictionary and reassigns their tasks.
    """
    while True:
        time.sleep(5)  # Run every 5 seconds
        current_time = time.time()

        with workers_lock:  # Acquire lock for thread-safe access
            inactive_workers = [
                worker_id for worker_id, details in workers.items()
                if current_time - details["last_heartbeat"] > WORKER_TIMEOUT
            ]

            for worker_id in inactive_workers:
                print(f"Worker {worker_id} is inactive. Reassigning its tasks.")
                workers.pop(worker_id, None)  # Remove inactive worker

                # Reassign tasks from this worker
                if worker_id in worker_task_map:
                    for task in worker_task_map.pop(worker_id, []):
                        task_queue.put(task)
                        print(f"Requeued task {task['taskId']} from worker {worker_id}.")


def handle_tasks():
    consumer = KafkaConsumer(
        TASK_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda m: json.loads(m.decode('ascii'))
    )

    for message in consumer:
        task = message.value
        print(f"Received task {task['taskId']}")

        # Add the task to the queue
        task_queue.put(task)


def __main__():
    # Run heartbeat, task handling, task assignment, and inactive worker checks in separate threads
    heartbeat_thread = threading.Thread(target=handle_heartbeats)
    task_thread = threading.Thread(target=handle_tasks)
    task_assignment_thread = threading.Thread(target=try_assign_task)
    inactive_worker_thread = threading.Thread(target=check_inactive_workers)

    heartbeat_thread.start()
    task_thread.start()
    task_assignment_thread.start()
    inactive_worker_thread.start()

    # Join threads to wait for them to finish (if desired)
    heartbeat_thread.join()
    task_thread.join()
    task_assignment_thread.join()
    inactive_worker_thread.join()


if __name__ == "__main__":
    __main__()
