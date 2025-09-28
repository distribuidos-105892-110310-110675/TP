from typing import Callable

import pika
from pika.exceptions import AMQPConnectionError, AMQPError

from middleware.middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)


class RabbitMQMessageMiddlewareQueue(MessageMiddlewareQueue):

    # ============================== PRIVATE - ACCESSING ============================== #

    def __rabbitmq_port(self) -> int:
        return 5672

    def __rabbitmq_user(self) -> str:
        return "guest"

    def __rabbitmq_password(self) -> str:
        return "guest"

    # ============================== PRIVATE - INITIALIZATION ============================== #

    def __init__(self, host, queue_name):
        super().__init__(host, queue_name)

        self._queue_name = queue_name
        self._exchange_name = ""

        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=host,
                    port=self.__rabbitmq_port(),
                    credentials=pika.PlainCredentials(
                        self.__rabbitmq_user(), self.__rabbitmq_password()
                    ),
                    heartbeat=3600,
                )
            )
            self._channel = self._connection.channel()
            self._channel.basic_qos(prefetch_count=1)
            self._channel.queue_declare(queue=queue_name, durable=True)
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(
                f"Error connecting to RabbitMQ server: {e}"
            )

    # ============================== PRIVATE - ASSERTIONS ============================== #

    def __assert_connection_is_open(self) -> None:
        if not self._connection.is_open or not self._channel.is_open:
            raise MessageMiddlewareDisconnectedError(
                "Error: Connection or channel is closed."
            )

    # ============================== PUBLIC ============================== #

    def start_consuming(self, on_message_callback: Callable) -> None:
        self.__assert_connection_is_open()

        def pika_on_message_callback(
            channel: pika.adapters.blocking_connection.BlockingChannel,
            method: pika.spec.Basic.Deliver,
            properties: pika.spec.BasicProperties,
            body: bytes,
        ) -> None:
            on_message_callback(body)
            channel.basic_ack(delivery_tag=method.delivery_tag)  # type: ignore

        try:
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=pika_on_message_callback,
                auto_ack=False,
            )
            self._channel.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error consuming messages: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error consuming messages: {e}")

    def stop_consuming(self) -> None:
        self.__assert_connection_is_open()
        self._channel.stop_consuming()

    def send(self, message: str) -> None:
        self.__assert_connection_is_open()
        try:
            self._channel.basic_publish(
                exchange=self._exchange_name,
                routing_key=self._queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),  # type: ignore
            )
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Error sending message: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error sending message: {e}")

    def close(self) -> None:
        try:
            self._channel.close()
            self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error closing connection: {e}")

    def delete(self) -> None:
        try:
            self._channel.queue_delete(
                queue=self._queue_name, if_unused=False, if_empty=False
            )
        except Exception as e:
            raise MessageMiddlewareDeleteError(f"Error deleting queue: {e}")
