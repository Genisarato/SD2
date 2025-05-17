import pika
import json
import boto3
import os

def invoke_delivery_lambda(message):
    """
    Invokes the Lambda function that simulates order delivery asynchronously.
    The Lambda function is expected to simulate a delay and process the order.
    """
    # Specify the region (e.g., us-east-1)
    lambda_client = boto3.client('lambda', region_name='us-east-1')
    payload = json.dumps({'order': message})
    try:
        response = lambda_client.invoke(
            FunctionName="lambda_delivery",
            InvocationType='Event',  # Asynchronous invocation
            Payload=payload
        )
        print("Delivery Lambda invoked for order:", message.get('order_id'))
    except Exception as e:
        print("Error invoking delivery Lambda for order:", message.get('order_id'), e)

def callback(ch, method, properties, body):
    try:
        # Convert the JSON message to a dictionary
        message = json.loads(body)
        print("Message received:", message)
        # Invoke the delivery Lambda function asynchronously
        invoke_delivery_lambda(message)
        # Acknowledge the message reception to remove it from the queue
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print("Error processing the message:", e)
        # Requeue the message for retry if processing fails
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def start_subscriber():
    # Configure the connection to RabbitMQ (modify host and credentials as needed)
    credentials = pika.PlainCredentials('user', 'password123')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='52.91.169.241', credentials=credentials)
    )
    channel = connection.channel()

    # Declare the queue (ensure the parameters match the existing queue)
    channel.queue_declare(queue='order_queue', durable=False)

    # Set up the subscriber with the callback to process each message
    channel.basic_consume(queue='order_queue', on_message_callback=callback)
    print("Subscriber active. Waiting for messages on the 'order_queue'...")

    try:
        # Start the message consumption loop
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Manual interruption. Closing subscriber...")
        channel.stop_consuming()
    finally:
        connection.close()

if __name__ == '__main__':
    start_subscriber()
