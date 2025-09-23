from typing import Callable, List

import pika

from middleware.middleware import MessageMiddlewareExchange


class RabbitMQMessageMiddlewareExchange(MessageMiddlewareExchange):
    def __init__(self, host: str, exchange_name: str, route_keys: List):
        super().__init__(host, exchange_name, route_keys)

        self._host = host
        self._exchange_name = exchange_name
        self._route_keys = route_keys

        connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        self._channel = connection.channel()

        self._channel.queue_declare(queue="hello")

        connection.close()

        pass

    def start_consuming(self, on_message_callback: Callable) -> None:
        pass

    def stop_consuming(self) -> None:
        pass

    def send(self, message: str) -> None:
        self._channel.basic_publish(exchange="", routing_key="hello", body=message)

    def close(self) -> None:
        pass

    def delete(self) -> None:
        pass
