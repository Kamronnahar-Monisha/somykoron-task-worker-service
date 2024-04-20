from flask import Flask,jsonify
import redis
import threading
import json
from dotenv import load_dotenv
import os
load_dotenv() 

app = Flask(__name__)
redis_client = redis.Redis(host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"))
#redis_client = redis.Redis(host='localhost', port=6379)



@app.route('/')
def home():
    return "welcome to worker service..."
        
    



def process_task_addition(task_data):
    # Process task here
    result = task_data['a'] + task_data['b']
    return result


def process_task_multiplication(task_data):
    # Process task here
    result = task_data['a'] * task_data['b']
    return result

def handle_message(message):
    print(message)
    if message['request'] == 'addition':
        result = process_task_addition(message)
    else:
        result = process_task_multiplication(message)
    print(result)
    return {'result': result}



def worker():
    while True:
        task_data = redis_client.rpop('tasks')
        if task_data:
            # deserialized the json string data 
            decentralized_data = json.loads(task_data.decode('utf-8'))
            print(decentralized_data)
            result = handle_message(decentralized_data)
            print(result)
            # Store result in Redis
            redis_client.lpush('results', json.dumps(result))

# def process_task_addition(task_data):
#     # Process task here
#     result = task_data['a'] + task_data['b']
#     return result


# def process_task_multiplication(task_data):
#     # Process task here
#     result = task_data['a'] * task_data['b']
#     return result

# def handle_message(message):
#     task_data = message['data'].decode('utf-8')
#     # deserialized the json string data 
#     deserialized_data = json.loads(task_data)
#     print(deserialized_data)
#     if deserialized_data['request'] == 'addition':
#         result = process_task_addition(deserialized_data)
#     else:
#         result = process_task_multiplication(deserialized_data)
#     print(result)
#     # Publish result to Redis channel
#     redis_client.publish('results', result)

# def worker():
#     # Subscribe to the 'tasks' channel
#     pubsub = redis_client.pubsub()
#     pubsub.subscribe('tasks')
#     for message in pubsub.listen():
#         if message['type'] == 'message':
#             handle_message(message)

if __name__ == '__main__':
    # Start multiple worker threads to handle messages concurrently
    num_workers = 3
    for _ in range(num_workers):
        threading.Thread(target=worker).start()
    app.run(debug=True)
