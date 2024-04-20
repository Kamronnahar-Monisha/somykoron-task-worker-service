from flask import Flask
import redis
import threading
import json

app = Flask(__name__)
redis_client = redis.Redis(host='localhost', port=6379)


def worker():
    # Subscribe to the 'tasks' channel
    pubsub = redis_client.pubsub()
    pubsub.subscribe('tasks')
    for message in pubsub.listen():
        if message['type'] == 'message':
            handle_message(message)

if __name__ == '__main__':
    # Start multiple worker threads to handle messages concurrently
    num_workers = 3
    for _ in range(num_workers):
        threading.Thread(target=worker).start()
    app.run(debug=True)
