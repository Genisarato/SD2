import pika
import concurrent.futures
import sys

# Función para enviar un mensaje a RabbitMQ
def send_message_to_rabbitmq(message, credentials, host='localhost', port=5672, queue='primitive_stream'):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host, port, '/', credentials))
    channel = connection.channel()

    # Publicar el mensaje en la cola de RabbitMQ
    channel.basic_publish(
        exchange='',
        routing_key=queue,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # Hacer que el mensaje sea persistente
        )
    )

    #print(f"S'ha enviat el missatge: {message}")
    connection.close()

def main():
    # Obtener el número de mensajes a enviar desde los parámetros de la línea de comandos
    if len(sys.argv) != 2:
        print("Por favor, proporciona el número de mensajes como argumento.")
        sys.exit(1)

    num_messages = int(sys.argv[1])
    
    # Credenciales para RabbitMQ (modificar si es necesario)
    credentials = pika.PlainCredentials('user', 'password123')

    # Crear un ThreadPoolExecutor para manejar los workers
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Enviar los mensajes de forma paralela (hasta un máximo de 10 workers)
        for i in range(num_messages):
            message = f'Tin Fill de puta'
            executor.submit(send_message_to_rabbitmq, message, credentials)
    
    print(f"Se han enviado {num_messages} mensajes.")

if __name__ == "__main__":
    main()
