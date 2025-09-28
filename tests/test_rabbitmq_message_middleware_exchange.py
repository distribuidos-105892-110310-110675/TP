import threading
import time

import pika

from middleware.rabbitmq_message_middleware_exchange import (
    RabbitMQMessageMiddlewareExchange,
)

RABBITMQ_HOST = "rabbitmq-dev"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASSWORD = "guest"


class TestRabbitMQMessageMiddlewareExchange:

    # ============================== PRIVATE - ACCESSING ============================== #

    def __rabbitmq_host(self) -> str:
        return "rabbitmq-dev"

    # ============================== PRIVATE - TESTS SUPPORT ============================== #

    def __consumer_handler_with_id(
        self,
        consumer_id: int,
        exchange_name: str,
        routing_keys: list,
        messages_received: dict,
        message_received_lock: threading.Lock,
    ) -> None:
        exchange_consumer = RabbitMQMessageMiddlewareExchange(
            self.__rabbitmq_host(), exchange_name, routing_keys
        )

        def on_message_callback(message_as_bytes: bytes) -> None:
            nonlocal messages_received
            message_received = message_as_bytes.decode("utf-8")

            with message_received_lock:
                messages_received[f"consumer_{consumer_id}"] = message_received

            exchange_consumer.stop_consuming()

        exchange_consumer.start_consuming(on_message_callback)

        exchange_consumer.close()

    def __spawn_exchange_consumers(
        self,
        number_of_consumers: int,
        exchange_name: str,
        routing_keys: list,
        messages_received: dict,
        message_received_lock: threading.Lock,
    ) -> list[threading.Thread]:
        spawned_threads: list[threading.Thread] = []

        for i in range(number_of_consumers):
            thread = threading.Thread(
                target=self.__consumer_handler_with_id,
                args=(
                    i,
                    exchange_name,
                    routing_keys,
                    messages_received,
                    message_received_lock,
                ),
            )
            thread.start()
            spawned_threads.append(thread)

        return spawned_threads

    def __test_working_exchange_communication_1_to(
        self, number_of_consumers: int, exchange_name: str, routing_keys: list
    ) -> None:
        message_published = "Testing message"

        messages_received = {f"consumer_{i}": "" for i in range(number_of_consumers)}
        messages_received_lock = threading.Lock()

        spawned_threads: list[threading.Thread] = self.__spawn_exchange_consumers(
            number_of_consumers,
            exchange_name,
            routing_keys,
            messages_received,
            messages_received_lock,
        )

        exchange_publisher = RabbitMQMessageMiddlewareExchange(
            self.__rabbitmq_host(), exchange_name, routing_keys
        )

        # we have to wait until all consumers are ready
        # because if we send before they are listening,
        # the message is lost (rabbitmq default behaviour)
        time.sleep(number_of_consumers)

        exchange_publisher.send(message_published)

        for thread in spawned_threads:
            thread.join()

        for i in range(number_of_consumers):
            assert messages_received[f"consumer_{i}"] == message_published

        exchange_publisher.delete()
        exchange_publisher.close()

    # ============================== TESTS ============================== #

    def test_working_exchange_communication_1_to_1(self) -> None:
        self.__test_working_exchange_communication_1_to(
            1, "exchange-communication-1-to-many", ["routing-key-1"]
        )

    def test_working_exchange_communication_1_to_many(self) -> None:
        self.__test_working_exchange_communication_1_to(
            5, "exchange-communication-1-to-many", ["routing-key-1"]
        )
