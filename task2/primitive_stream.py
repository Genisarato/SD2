import pika
import threading
import time
import logging
from datetime import datetime
import csv

logging.basicConfig(level=logging.INFO, format='%(threadName)s %(message)s')

#Funció d'exemple
def print_message(message):
	print(f"El missatge es: {message}")

#Funció "lambda"/worker
def worker(function, queue_name, stop_event):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_qos(prefetch_count=1)

    #S'executa normal com un consumer de RabbitMQ mentre no se li indica el stop_event
    def callback(ch, method, properties, body):
        if stop_event.is_set():
            ch.basic_nack(method.delivery_tag, requeue=True)
            ch.stop_consuming()
            return
        message = body.decode()
        try:
            function(message)
            ch.basic_ack(method.delivery_tag)
        except Exception as e:
            logging.error(f"Error processant missatge: {e}")
            ch.basic_nack(method.delivery_tag, requeue=True)

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

    while not stop_event.is_set():
        connection.process_data_events(time_limit=1)

    try:
        channel.close()
        connection.close()
    except Exception:
        pass
    logging.info("Worker aturat")

#Funció per obtenir els missatges de la cua i poder llençar els workers segons
def get_queue_message_count(queue_name):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    q = channel.queue_declare(queue=queue_name, durable=True, passive=True)
    message_count = q.method.message_count
    connection.close()
    return message_count

 #Manegador principal dels threads
def primitive_stream(function, max_workers, queue_name):
    workers = []
    stop_events = []
    stats_log = []

    try:
        while True:
            pending = get_queue_message_count(queue_name)
            active = sum(1 for w in workers if w.is_alive())
            target = min(max_workers, max(1, pending))

            # Crear workers si cal
            if active < target:
                for _ in range(target - active):
                    stop_event = threading.Event()
                    t = threading.Thread(target=worker, args=(function, queue_name, stop_event), daemon=True)
                    t.start()
                    workers.append(t)
                    stop_events.append(stop_event)
                logging.info(f"Creant threads. Actius: {len([w for w in workers if w.is_alive()])}")

            # Aturar workers si sobren
            elif active > target:
                to_stop = active - target
                for _ in range(to_stop):
                    stop_events.pop().set()

            # Neteja workers morts
            workers = [w for w in workers if w.is_alive()]

            stats_log.append( (datetime.now(), len(workers)) )
            time.sleep(1)

    #Acabem el procés en background
    except KeyboardInterrupt:
        logging.info("Aturant tot...")

        # Parar tots els workers
        for e in stop_events:
            e.set()

        for w in workers:
            w.join()

    # Guardar estadístiques
    with open('stats_workers.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'active_workers'])
        for ts, count in stats_log:
            writer.writerow([ts.isoformat(), count])

    logging.info("Estadístiques de workers guardades a stats_workers.csv")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--function', default='print_message')
    parser.add_argument('--maxworkers', type=int, default=10)
    parser.add_argument('--queue', default='primitive_stream')
    args = parser.parse_args()

    func_map = {
        'print_message': print_message,
    }
    func_to_use = func_map.get(args.function)
    #func_to_use = func_map.get(args.function, example_function)

    primitive_stream(func_to_use, args.maxworkers, args.queue)
