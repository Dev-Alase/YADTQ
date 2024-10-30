
from redis import Redis
import json

from yadtq.config import REDIS_HOST,REDIS_PORT,REDIS_DB

class ResultBackend :

    def __init__(self) :
        self.redis_client = Redis(REDIS_HOST, REDIS_PORT ,REDIS_DB, decode_responses=True)

    def submit_task(self,taskId,task) :
        self.redis_client.hset(taskId, mapping = task)

        return

    def query_tasks(self,taskId="") :
        if taskId:
            task_data = self.redis_client.hgetall(taskId)
            if task_data:
                print(f"Task {taskId} details: {task_data}")
            else:
                print(f"Task with ID {taskId} not found.")
        else:
            all_task_keys = self.redis_client.keys(pattern="task*")
            for key in all_task_keys:
                print(f"{key}: {self.redis_client.hgetall(key)}")

        return

    def clear(self) :
        self.redis_client.flushdb()
