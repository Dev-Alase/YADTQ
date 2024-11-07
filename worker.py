from kafka import KafkaConsumer, KafkaProducer
import json
from yadtq.config import KAFKA_BROKER_URL, TASK_TOPIC, HEARTBEAT_TOPIC, WORKER_TOPIC
from backend import ResultBackend
import time
import uuid
import threading
    
# Initialize the ResultBackend to interact with Redis
rb = ResultBackend()

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda m: json.dumps(m).encode('ascii')
)

class TaskProcessor:
    def process_task(self, task):
        """
        Process the task based on the type (ADD, SUB, MUL).
        Returns the result of the computation.
        """
        task_type = task['task'].upper()
        op1 = int(task['operand 01'])
        op2 = int(task['operand 02'])

        if task_type == "ADD":
            result = op1 + op2
        elif task_type == "SUB":
            result = op1 - op2
        elif task_type == "MUL":
            result = op1 * op2
        else:
            raise ValueError("Unsupported task type")

        time.sleep(30)

        return result

    def handle_task(self, task):
        """
        Processes the task and updates Redis with the result and status.
        """
        task_id = task['taskId']

        # Check if the task is already processed (for idempotency)
        task_data = rb.query_tasks(task_id)
        if task_data and task_data.get('status') == 'completed':
            print(f"Task {task_id} is already completed. Skipping...")
            return

        # Update task status to "in-progress" in Redis
        rb.update_task_status(task_id, 'in-progress')

        try:
            # Process the task and get the result
            result = self.process_task(task)

            # Update task status to "completed" and store result in Redis
            rb.update_task_status(task_id, 'completed', result=result)
            print(f"Task {task_id} processed and updated in Redis with result.")
        except Exception as e:
            print(f"Error processing task {task_id}: {e}")
            rb.update_task_status(task_id,'failed',result=str(e))
            
            # Optionally update the status to "failed" or handle retries as needed

worker_status = "available"
worker_status_lock = threading.Lock()  # Lock to ensure thread-safe updates to worker status

def send_heartbeats(worker_id):
    while True:
        with worker_status_lock:
            heartbeat_data = {"worker_id": str(worker_id), "status": worker_status}
        producer.send(HEARTBEAT_TOPIC, heartbeat_data)
        producer.flush()  # Ensure message is sent immediately
        time.sleep(5)

def worker(worker_id):
    print(f'Worker started with ID: {worker_id}, waiting for tasks...')

    task_processor = TaskProcessor()

    # Create a unique group_id for each worker
    group_id = f"worker_group_{worker_id}"  # Unique consumer group per worker

    consumer = KafkaConsumer(
    WORKER_TOPIC,
    bootstrap_servers=KAFKA_BROKER_URL,
    group_id=group_id,  # Use a unique group ID per worker
    value_deserializer=lambda m: json.loads(m.decode('ascii'))
    )

    for message in consumer:
        # print(message)
        task_message = message.value
        task = task_message.get('task')
        assigned_worker_id = task_message.get('worker_id')

        if assigned_worker_id == str(worker_id):
            print(f"Assigned task {task['taskId']} to worker {worker_id}: {task}")
            
            with worker_status_lock:
                global worker_status
                worker_status = "busy"  # Mark worker as busy when processing a task

            # Handle the task and update Redis with the result
            
            task_processor.handle_task(task)


            with worker_status_lock:
                worker_status = "available"  # Mark worker as available once task is completed



if __name__ == "__main__":
    worker_id = uuid.uuid4()  # Generate a unique ID for the worker
    
    # Start heartbeat in a separate thread
    heartbeat_thread = threading.Thread(target=send_heartbeats, args=(worker_id,))
    heartbeat_thread.daemon = True  # Daemonize thread to exit when the main program exits
    heartbeat_thread.start()

    try:
        worker(worker_id)
    except KeyboardInterrupt:
        print("Worker stopped.")
