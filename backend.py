from redis import Redis
from yadtq.config import REDIS_HOST, REDIS_PORT, REDIS_DB

class ResultBackend:
    def __init__(self):
        self.redis_client = Redis(REDIS_HOST, REDIS_PORT, REDIS_DB, decode_responses=True)

    def submit_task(self, taskId, task):
        """Adds a new task with its initial details."""
        self.redis_client.hset(taskId, mapping=task)

    def update_task_status(self, taskId, status, result=None):
        """Updates the status and result (if provided) of a task within the structured task data."""
        task_update = {'status': status}
        if result is not None:
            task_update['result'] = result
        self.redis_client.hset(taskId, mapping=task_update)

    def query_tasks(self, taskId=""):
        """
        Fetches a specific task by taskId or lists all tasks.
        Returns task details as a dictionary if taskId is provided,
        or None if the task doesn't exist.
        """
        if taskId:
            task_data = self.redis_client.hgetall(taskId)
            if task_data:
                return task_data  # Return full task data as a dictionary
            else:
                print(f"Task with ID {taskId} not found.")
                return None  # Return None if task not found
        else:
            all_task_keys = sorted(self.redis_client.keys(pattern="task*"))
            for key in all_task_keys:
                print(f"{key}: {self.redis_client.hgetall(key)}")

    def clear(self):
        """Clears all tasks in Redis (for cleanup purposes)."""
        self.redis_client.flushdb()
