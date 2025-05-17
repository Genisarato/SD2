import pika

credentials = pika.PlainCredentials('user', 'password123')
connection = pika.BlockingConnection(pika.ConnectionParameters('54.167.85.155', 5672, '/', credentials))
channel = connection.channel()

channel.queue_declare(queue='InsultQueue', durable=True) 

connection.close()
