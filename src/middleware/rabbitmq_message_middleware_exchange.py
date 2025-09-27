import logging
from typing import Callable, List

import pika

from middleware.middleware import MessageMiddlewareCloseError, MessageMiddlewareExchange


class RabbitMQMessageMiddlewareExchange(MessageMiddlewareExchange):

    # ============================== PRIVATE - ACCESSING ============================== #

    def __rabbitmq_port(self) -> int:
        return 5672

    def __rabbitmq_user(self) -> str:
        return "guest"

    def __rabbitmq_password(self) -> str:
        return "guest"

    # ============================== PRIVATE - SUPPORT ============================== #

    def __bind_queue_to_routing_keys(self, queue_name: str) -> None:
        for routing_key in self._routing_keys:
            self._channel.queue_bind(
                exchange=self._exchange_name,
                queue=queue_name,
                routing_key=routing_key,
            )

    # ============================== PRIVATE - INITIALIZATION ============================== #

    def __init__(self, host: str, exchange_name: str, route_keys: List):
        super().__init__(host, exchange_name, route_keys)

        self._exchange_name = exchange_name
        self._routing_keys = route_keys

        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=host,
                port=self.__rabbitmq_port(),
                credentials=pika.PlainCredentials(
                    self.__rabbitmq_user(), self.__rabbitmq_password()
                ),
            )
        )

        self._channel = self._connection.channel()
        self._channel.exchange_declare(
            exchange=self._exchange_name,
            exchange_type="direct",  # type: ignore
            durable=True,
        )

        # add_on_cancel_callback, add_on_close_callback could be useful
        # basic_ack, basic_nack, basic_reject could be useful

    def start_consuming(self, on_message_callback: Callable) -> None:
        result = self._channel.queue_declare(
            queue="", exclusive=True, auto_delete=True, durable=True
        )
        # maybe can be exclusive, auto_delete, durable, etc
        queue_name = str(result.method.queue)  # see if necessary the variable
        self.__bind_queue_to_routing_keys(queue_name)

        def pika_on_message_callback(
            channel: pika.adapters.blocking_connection.BlockingChannel,
            method: pika.spec.Basic.Deliver,
            properties: pika.spec.BasicProperties,
            body: bytes,
        ) -> None:
            try:
                on_message_callback(body)
                channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)  # type: ignore
            except Exception as e:
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)  # type: ignore
                # @TODO change print
                logging.error(f"Error processing message: {e}")

        self._channel.basic_consume(
            queue=queue_name,
            on_message_callback=pika_on_message_callback,
            auto_ack=False,
        )
        self._channel.start_consuming()

    def stop_consuming(self) -> None:
        self._channel.stop_consuming()

    def send(self, message: str) -> None:
        for routing_key in self._routing_keys:
            self._channel.basic_publish(
                exchange=self._exchange_name,
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),  # type: ignore
            )

    def close(self) -> None:
        # @TODO: add test
        try:
            self._channel.close()
            self._connection.close()
        except BaseException as e:
            raise MessageMiddlewareCloseError(f"Error closing connection: {e}")

    def delete(self) -> None:
        self._channel.exchange_delete(exchange=self._exchange_name, if_unused=False)
