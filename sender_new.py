import pika
import json
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()


channel.queue_declare(queue='message')
path = input("Enter db path: ").lower()
formating = input("Enter the output file format: ").lower()


data = {
    "path" : path,
    "format": formating
    }
message = json.dumps(data)
channel.basic_publish(exchange='',
                      routing_key='message',
                      body=message)
print(" [x] Sent data")
connection.close()

