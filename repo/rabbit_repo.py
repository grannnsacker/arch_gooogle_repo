import asyncio
import threading

import pika
import json

from repo import get_spreadsheet_rep
from repo.spreadsheet_repo import SpreadsheetRepository


class RabbitMqRepository:
    def __init__(self, read_queue_name, write_queue_name, rabbitmq_host = 'localhost'):
        self.read_queue_name = read_queue_name
        self.write_queue_name = write_queue_name
        self.rabbitmq_host = rabbitmq_host
        self.stop_event = threading.Event()
        self.funcs = {
            "init_name": SpreadsheetRepository.init_name,
            "append_from_end": SpreadsheetRepository.append_from_end,
            "update_cell_in_column_a": SpreadsheetRepository.update_cell_in_column_a,
            "get_points": SpreadsheetRepository.get_points,
            "write_end_time": SpreadsheetRepository.write_end_time,
        }

    def start(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(self.rabbitmq_host))
        channel = connection.channel()
        channel.queue_declare(queue=self.read_queue_name, durable=True)

        def callback(ch, method, properties, body):
            message = json.loads(body.decode())
            repo = get_spreadsheet_rep()

            result = asyncio.run(repo.parse_method(message["func_name"], *message["args"]))
            if message["func_name"] == "get_points":
                self.send_message({"points": result})
            print(f"Получено сообщение: {message}")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=self.read_queue_name, on_message_callback=callback)

        print("Ожидание сообщений...")
        try:
            while not self.stop_event.is_set():  # Проверяем флаг в цикле
                channel.start_consuming()
        except KeyboardInterrupt:
            print("Остановка потребителя...")
            channel.stop_consuming()
        finally:
            connection.close()

    def stop(self):
        self.stop_event.set()

    def send_message(self, message):
        message_json = json.dumps(message)

        connection = pika.BlockingConnection(pika.ConnectionParameters(self.rabbitmq_host))
        channel = connection.channel()
        channel.queue_declare(queue=self.write_queue_name, durable=True)
        channel.basic_publish(
            exchange='',
            routing_key=self.write_queue_name,
            body=message_json,
            properties=pika.BasicProperties(
                delivery_mode=2,
            )
        )
        print(f"Отправлено сообщение: {message}")
        connection.close()