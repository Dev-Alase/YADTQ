
from kafka import KafkaProducer
import sys
import termios
import redis
import json
from yadtq.config import KAFKA_BROKER_URL,TASK_TOPIC
from backend import ResultBackend
# import time # uuid,secrets,hashlib

rb = ResultBackend()
producer = KafkaProducer(bootstrap_servers = KAFKA_BROKER_URL,value_serializer = lambda m : json.dumps(m).encode('ascii'))

class TaskIDGenerator:
    def __init__(self, start=1):
        self.counter = start

    def get_task_id(self):
        task_id = f"task{str(self.counter).zfill(3)}"
        self.counter += 1
        return task_id


def submit(task_id_gen) :

    print("\nThe tasks supported by the system are ADD, SUB, and MUL ")
    print("Let your input format be TASK OP1 OP2 (ADD 2 3)\n\n")

    print(" -> ",end="")

    task = ""

    task = input()
    task = task.strip().split(" ")
    op1 = task[1]
    op2 = task[2]

    task = task[0]
    taskId = task_id_gen.get_task_id()


    if len(task) < 3 :
        print("Please enter a task in a valid format....")
        return


    new_task = {
        'taskId' : taskId,
        'task' : task,
        'operand 01' : op1,
        'operand 02' : op2,
        'result' : 'null',
        'status' : 'queued'
    }

    rb.submit_task(taskId,new_task)
    producer.send(TASK_TOPIC,new_task)

    print(f'Task successfully created with the taskId : {taskId}')

    print("\nPlease enter your next task\n")

    return


def query(taskId=""):

    rb.query_tasks(taskId)

    print("\nPlease enter your next task\n")

    return



def __main__() :

    task_id_gen = TaskIDGenerator()

    print(" Client Program......  ")
    print(" Provide the information about the task you want to perform : ")
    print(" 1. enter ")
    print(" 2. query ")
    print(" 3. query [taskId]")
    print(" 4. clear")
    print(" 4. quit \n\n")

    proc = ""

    while proc != "quit" :
        print(" -> ",end="")
        proc = input()


        proc = proc.strip().split(" ")
        taskId  = proc[1] if len(proc) >= 2 else ""
        proc = proc[0]

        termios.tcflush(sys.stdin, termios.TCIOFLUSH)

        if proc == "enter" :
            submit(task_id_gen)
        elif proc == "query" :
            query(taskId)
        elif proc == "clear" :
            rb.clear()
        elif proc == "quit" :
            print("\n\nClinet executed successfully.....")
            print("Now aborting.....")
        else :
            print("Provide the correct task")

    rb.clear()


if __name__ == "__main__":
    __main__()
