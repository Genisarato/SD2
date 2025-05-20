import pika
import json
import boto3
import os

def invoke_delivery_lambda(message):
    """
    Invoca la funció Lambda que simula l'entrega de la comanda de manera asíncrona.
    Aquesta funció Lambda es suposa que simula un retard i processa la comanda.
    """
    # Creem el client boto3 per Lambda indicant la regió (per exemple, us-east-1)
    lambda_client = boto3.client('lambda', region_name='us-east-1')
    # Convertim el missatge a JSON per enviar com a payload
    payload = json.dumps({'order': message})
    try:
        # Invocar la Lambda de manera asíncrona (InvocationType='Event')
        response = lambda_client.invoke(
            FunctionName="lambda_delivery",  # Nom de la funció Lambda
            InvocationType='Event',           # Invocació asíncrona
            Payload=payload                   # Dades de la comanda
        )
        print("Delivery Lambda invoked for order:", message.get('order_id'))
    except Exception as e:
        # En cas d’error al invocar la Lambda, mostrar missatge
        print("Error invoking delivery Lambda for order:", message.get('order_id'), e)

def callback(ch, method, properties, body):
    try:
        # Convertim el missatge JSON rebut a diccionari Python
        message = json.loads(body)
        print("Message received:", message)
        # Invocar la funció Lambda que simula l'entrega de la comanda
        invoke_delivery_lambda(message)
        # Confirmar la recepció del missatge i eliminar-lo de la cua
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        # En cas d’error en el processament, mostrar missatge i tornar a posar el missatge a la cua
        print("Error processing the message:", e)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def start_subscriber():
    # Configuració de la connexió a RabbitMQ (canvia host i credencials segons calgui)
    credentials = pika.PlainCredentials('user', 'password123')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', credentials=credentials)
    )
    channel = connection.channel()

    # Declarem la cua 'order_queue' (sense durabilitat)
    channel.queue_declare(queue='order_queue', durable=False)

    # Configurem el consumidor, passant la funció callback per processar missatges
    channel.basic_consume(queue='order_queue', on_message_callback=callback)
    print("Subscriber active. Waiting for messages on the 'order_queue'...")

    try:
        # Iniciem el loop per consumir missatges indefinidament
        channel.start_consuming()
    except KeyboardInterrupt:
        # Permet aturar el consumidor manualment (Ctrl+C)
        print("Manual interruption. Closing subscriber...")
        channel.stop_consuming()
    finally:
        # Tanquem la connexió al final o quan s'aturi el consumidor
        connection.close()

if __name__ == '__main__':
    # Punt d'entrada del script: iniciem el subscriber
    start_subscriber()
