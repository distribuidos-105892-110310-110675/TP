from typing import Callable, List

import pika

from middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue


class RabbitMQMessageMiddlewareQueue(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        super().__init__(host, queue_name)
        pass

    def start_consuming(self, on_message_callback: Callable) -> None:
        pass

    def stop_consuming(self) -> None:
        pass

    def send(self, message: str) -> None:
        pass

    def close(self) -> None:
        pass

    def delete(self) -> None:
        pass
