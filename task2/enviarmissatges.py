import pika
import concurrent.futures
import sys

# Funció per a enviar missatges a la cua i simular l'escalat
def send_message_to_rabbitmq(message, credentials, host='localhost', port=5672, queue='primitive_stream'):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host, port, '/', credentials))
    channel = connection.channel()

    # Publiquem missatge a la cua RabbitMQ
    channel.basic_publish(
        exchange='',
        routing_key=queue,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # Missatges persistents
        )
    )

    connection.close()

def main():
    if len(sys.argv) != 2:
        print("Por favor, proporciona el número de mensajes como argumento.")
        sys.exit(1)

    num_messages = int(sys.argv[1])

    credentials = pika.PlainCredentials('user', 'password123')

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for i in range(num_messages):
            message = f'Fill de puta'
            executor.submit(send_message_to_rabbitmq, message, credentials)
    
    print(f"S'han enviat {num_messages} missatges.")

if __name__ == "__main__":
    main()
