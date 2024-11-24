import time

from repo.rabbit_repo import RabbitMqRepository
import threading


rabbitmq_host = 'localhost'
read_queue_name = 'bot_spreadsheet'
write_queue_name = 'spreadsheet_bot'
rr = RabbitMqRepository(read_queue_name, write_queue_name, rabbitmq_host)

thread_one = threading.Thread(target=rr.start)

try:
    thread_one.start()
except KeyboardInterrupt:
    rr.stop()
    thread_one.join()

