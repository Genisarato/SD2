import pika
import argparse

def crear_cua(queue_name):

	credentials = pika.PlainCredentials('user', 'password123')
	connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', credentials))
	channel = connection.channel()

	channel.queue_declare(queue=queue_name, durable=True) 

	connection.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Crear una cua RabbitMQ')
    parser.add_argument('--queue_name', type=str, required=True, help='Nom de la cua a crear')

    args = parser.parse_args()
    crear_cua(args.queue_name)
