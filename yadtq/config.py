
COORDINATOR_IP = "0.0.0.0"

KAFKA_BROKER_URL = f"{COORDINATOR_IP}:9092"
TASK_TOPIC = "tasks"
WORKER_TOPIC = "worker"
HEARTBEAT_TOPIC = "heartbeat"

REDIS_HOST = COORDINATOR_IP
REDIS_PORT = 6379
REDIS_DB = 0
