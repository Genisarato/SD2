import logging
import pika
import boto3
import json

# Configurar logging para escriure logs i contrasenya
logging.basicConfig(
    filename='logsTrigger.txt',          # Logs
    level=logging.INFO,                  # Nivell del missatge
    format='%(asctime)s - %(levelname)s - %(message)s'  # Formato
)

# Creem client de lambda per a poder fer de trigger
lambda_client = boto3.client('lambda', region_name='us-east-1')

def invoke_lambda(payload):
    # Invoquem sense esperar resposta de manera asincrónica
    response = lambda_client.invoke(
        FunctionName='Filter_Function',  # Nom de la lambda
        InvocationType='Event',  # Forma asíncrona
        Payload=json.dumps(payload)  # 
    )
# Conexió amb RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672, '/', pika.PlainCredentials('user', 'password123')))
channel = connection.channel()

# Funció que executarà RabbitMQ
def callback(ch, method, properties, body):
    # Decodificar el mensaje
    message = body.decode()
    logging.info(f"Missatge a enviar a lambda: {message}")  # Registrem que enviem

    # Creem el json que enviarem al missatge
    payload = {
        "missatge_original": message,
     }

    # Invocar Lambda
    invoke_lambda(payload)

    # Confirmem missatge
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue='InsultQueue', on_message_callback=callback)



# Iniciem el trigger
channel.start_consuming()

connection.close()
