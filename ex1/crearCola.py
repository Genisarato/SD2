import pika

credentials = pika.PlainCredentials('user', 'password123')
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
channel = connection.channel()

channel.queue_declare(queue='InsultQueue', durable=True) 

connection.close()
