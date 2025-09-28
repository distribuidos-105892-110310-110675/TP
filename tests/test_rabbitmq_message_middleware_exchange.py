import threading
import time

import pytest

from middleware.middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
)
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
        messages_received_lock: threading.Lock,
    ) -> None:
        exchange_consumer = RabbitMQMessageMiddlewareExchange(
            self.__rabbitmq_host(), exchange_name, routing_keys
        )

        def on_message_callback(message_as_bytes: bytes) -> None:
            nonlocal messages_received
            message_received = message_as_bytes.decode("utf-8")

            with messages_received_lock:
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
        messages_received_lock: threading.Lock,
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
                    messages_received_lock,
                ),
            )
            thread.start()
            spawned_threads.append(thread)

        return spawned_threads

    def __test_direct_working_exchange_communication_1_to(
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

        # we have to wait until all consumers are ready
        # because if we send before they are listening,
        # the message is lost (rabbitmq default behaviour)
        time.sleep(number_of_consumers)

        exchange_publisher = RabbitMQMessageMiddlewareExchange(
            self.__rabbitmq_host(), exchange_name, routing_keys
        )
        exchange_publisher.send(message_published)

        for thread in spawned_threads:
            thread.join()

        for i in range(number_of_consumers):
            assert messages_received[f"consumer_{i}"] == message_published

        exchange_publisher.delete()
        exchange_publisher.close()

    # ============================== TESTS - COMMUNICATION ============================== #

    def test_direct_working_exchange_communication_1_to_1(self) -> None:
        self.__test_direct_working_exchange_communication_1_to(
            1, "exchange-communication-1-to-many-direct", ["routing-key.1"]
        )

    def test_direct_working_exchange_communication_1_to_many(self) -> None:
        self.__test_direct_working_exchange_communication_1_to(
            5, "exchange-communication-1-to-many-direct", ["routing-key.1"]
        )

    def test_topic_working_exchange_communication_1_to_many(self) -> None:
        number_of_consumers = 3
        exchange_name = "exchange-communication-many-to-many-topic"

        message_published = "Testing message"

        messages_received = {f"consumer_{i}": "" for i in range(number_of_consumers)}
        messages_received_lock = threading.Lock()

        spawned_threads: list[threading.Thread] = []

        thread = threading.Thread(
            target=self.__consumer_handler_with_id,
            args=(
                0,
                exchange_name,
                ["routing-key.1"],
                messages_received,
                messages_received_lock,
            ),
        )
        thread.start()
        spawned_threads.append(thread)

        thread = threading.Thread(
            target=self.__consumer_handler_with_id,
            args=(
                1,
                exchange_name,
                ["routing-key.*"],
                messages_received,
                messages_received_lock,
            ),
        )
        thread.start()
        spawned_threads.append(thread)

        thread = threading.Thread(
            target=self.__consumer_handler_with_id,
            args=(
                2,
                exchange_name,
                ["*.1"],
                messages_received,
                messages_received_lock,
            ),
        )
        thread.start()
        spawned_threads.append(thread)

        # we have to wait until all consumers are ready
        # because if we send before they are listening,
        # the message is lost (rabbitmq default behaviour)
        time.sleep(number_of_consumers)

        exchange_publisher = RabbitMQMessageMiddlewareExchange(
            self.__rabbitmq_host(), exchange_name, ["routing-key.1"]
        )
        exchange_publisher.send(message_published)

        for thread in spawned_threads:
            thread.join()

        for i in range(number_of_consumers):
            assert messages_received[f"consumer_{i}"] == message_published

        exchange_publisher.delete()
        exchange_publisher.close()

    def test_topic_working_exchange_communication_many_to_many(self) -> None:
        number_of_consumers = 3
        exchange_name = "exchange-communication-many-to-many-topic"

        message_published_0 = "Testing message 0"
        message_published_1 = "Testing message 1"

        messages_received = {f"consumer_{i}": "" for i in range(number_of_consumers)}
        messages_received_lock = threading.Lock()

        spawned_threads: list[threading.Thread] = []

        thread = threading.Thread(
            target=self.__consumer_handler_with_id,
            args=(
                0,
                exchange_name,
                ["*.1"],
                messages_received,
                messages_received_lock,
            ),
        )
        thread.start()
        spawned_threads.append(thread)

        thread = threading.Thread(
            target=self.__consumer_handler_with_id,
            args=(
                1,
                exchange_name,
                ["*.2"],
                messages_received,
                messages_received_lock,
            ),
        )
        thread.start()
        spawned_threads.append(thread)

        # we have to wait until all consumers are ready
        # because if we send before they are listening,
        # the message is lost (rabbitmq default behaviour)
        time.sleep(number_of_consumers)

        exchange_publisher = RabbitMQMessageMiddlewareExchange(
            self.__rabbitmq_host(), exchange_name, ["routing-key.1"]
        )
        exchange_publisher.send(message_published_0)

        exchange_publisher = RabbitMQMessageMiddlewareExchange(
            self.__rabbitmq_host(), exchange_name, ["routing-key.2"]
        )
        exchange_publisher.send(message_published_1)

        for thread in spawned_threads:
            thread.join()

        assert messages_received["consumer_0"] == message_published_0
        assert messages_received["consumer_1"] == message_published_1

        exchange_publisher.delete()
        exchange_publisher.close()

    # ============================== TESTS - FUNCTIONS ============================== #

    def test_stop_consuming_multiple_times(self) -> None:
        exchange_consumer = RabbitMQMessageMiddlewareExchange(
            self.__rabbitmq_host(),
            "exchange-stop-consuming-multiple-times",
            ["routing-key.1"],
        )

        exchange_consumer.stop_consuming()
        exchange_consumer.stop_consuming()

        exchange_consumer.delete()
        exchange_consumer.close()

    # ============================== TESTS - EXCEPTIONS ============================== #

    def test_error_while_creating_connection_with_wrong_host(self) -> None:
        with pytest.raises(MessageMiddlewareDisconnectedError) as exc_info:
            RabbitMQMessageMiddlewareExchange(
                "non-existing-host",
                "exchange-error-while-creating-connection-with-wrong-host",
                ["routing-key.1"],
            )

        assert (
            "Error connecting to RabbitMQ server: [Errno -2] Name or service not known"
            == str(exc_info.value)
        )

    def test_error_while_starting_consuming_from_closed_connection(self) -> None:
        middleware = RabbitMQMessageMiddlewareExchange(
            self.__rabbitmq_host(), "testing-error-exchange", ["routing-key.1"]
        )
        middleware.delete()
        middleware.close()

        with pytest.raises(MessageMiddlewareDisconnectedError) as exc_info:
            middleware.start_consuming(lambda msg: None)

        assert str(exc_info.value) == "Error: Connection or channel is closed."

    def test_consumer_callback_failure_raises_exception(self) -> None:
        def send_with_delay() -> None:
            # we have to wait until all consumers are ready
            # because if we send before they are listening,
            # the message is lost (rabbitmq default behaviour)
            time.sleep(1)

            exchange_publisher = RabbitMQMessageMiddlewareExchange(
                self.__rabbitmq_host(), "testing-error-exchange", ["routing-key.1"]
            )
            exchange_publisher.send("message that will cause failure")
            exchange_publisher.close()

        thread = threading.Thread(target=send_with_delay)
        thread.start()

        exchange_consumer = RabbitMQMessageMiddlewareExchange(
            self.__rabbitmq_host(), "testing-error-exchange", ["routing-key.1"]
        )

        def failing_callback(message_as_bytes: bytes) -> None:
            raise ValueError("Simulated processing error")

        with pytest.raises(MessageMiddlewareMessageError) as exc_info:
            exchange_consumer.start_consuming(failing_callback)

        assert (
            str(exc_info.value)
            == "Error consuming messages: Simulated processing error"
        )

        thread.join()

        exchange_consumer.delete()
        exchange_consumer.close()

    def test_error_while_stopping_consuming_from_closed_connection(self) -> None:
        middleware = RabbitMQMessageMiddlewareExchange(
            self.__rabbitmq_host(), "testing-error-exchange", ["routing-key.1"]
        )
        middleware.delete()
        middleware.close()

        with pytest.raises(MessageMiddlewareDisconnectedError) as exc_info:
            middleware.stop_consuming()

        assert str(exc_info.value) == "Error: Connection or channel is closed."

    def test_error_while_sending_msg_to_closed_connection(self) -> None:
        middleware = RabbitMQMessageMiddlewareExchange(
            self.__rabbitmq_host(), "testing-error-exchange", ["routing-key.1"]
        )
        middleware.delete()
        middleware.close()

        with pytest.raises(MessageMiddlewareDisconnectedError) as exc_info:
            middleware.send("Testing message")

        assert str(exc_info.value) == "Error: Connection or channel is closed."

    def test_error_while_sending_wrong_msg(self) -> None:
        middleware = RabbitMQMessageMiddlewareExchange(
            self.__rabbitmq_host(), "testing-error-exchange", ["routing-key.1"]
        )

        with pytest.raises(MessageMiddlewareMessageError) as exc_info:
            middleware.send(None)  # type: ignore

        assert (
            str(exc_info.value)
            == "Error sending message: object of type 'NoneType' has no len()"
        )

        middleware.delete()
        middleware.close()

    def test_error_while_closing_already_closed_connection(self) -> None:
        middleware = RabbitMQMessageMiddlewareExchange(
            self.__rabbitmq_host(), "testing-error-exchange", ["routing-key.1"]
        )
        middleware.delete()
        middleware.close()

        with pytest.raises(MessageMiddlewareCloseError) as exc_info:
            middleware.close()

        assert str(exc_info.value) == "Error closing connection: Channel is closed."

    def test_error_while_deleting_from_closed_connection(self) -> None:
        middleware = RabbitMQMessageMiddlewareExchange(
            self.__rabbitmq_host(), "testing-error-exchange", ["routing-key.1"]
        )
        middleware.delete()
        middleware.close()

        with pytest.raises(MessageMiddlewareDeleteError) as exc_info:
            middleware.delete()

        assert str(exc_info.value) == "Error deleting queue: Channel is closed."
