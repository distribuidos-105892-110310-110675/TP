import threading

import pytest

from middleware.middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
)
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue


class TestRabbitMQMessageMiddlewareQueue:

    # ============================== PRIVATE - ACCESSING ============================== #

    def _rabbitmq_host(self) -> str:
        return "rabbitmq-dev"

    # ============================== PRIVATE - TESTS SUPPORT ============================== #

    def _consumer_handler_with_id(
        self,
        consumer_id: int,
        queue_name: str,
        messages_received: dict,
        message_received_lock: threading.Lock,
    ) -> None:
        queue_consumer = RabbitMQMessageMiddlewareQueue(
            self._rabbitmq_host(), queue_name
        )

        def on_message_callback(message_as_bytes: bytes) -> None:
            nonlocal messages_received
            message_received = message_as_bytes.decode("utf-8")

            with message_received_lock:
                messages_received[f"consumer_{consumer_id}"] = message_received

            queue_consumer.stop_consuming()

        queue_consumer.start_consuming(on_message_callback)

        queue_consumer.close()

    def _spawn_queue_consumers(
        self,
        number_of_consumers: int,
        queue_name: str,
        messages_received: dict,
        message_received_lock: threading.Lock,
    ) -> list[threading.Thread]:
        spawned_threads: list[threading.Thread] = []

        for i in range(number_of_consumers):
            thread = threading.Thread(
                target=self._consumer_handler_with_id,
                args=(i, queue_name, messages_received, message_received_lock),
            )
            thread.start()
            spawned_threads.append(thread)

        return spawned_threads

    def _test_working_queue_communication_1_to(
        self, number_of_consumers: int, queue_name: str
    ) -> None:
        message_published = "Testing message"

        messages_received = {f"consumer_{i}": "" for i in range(number_of_consumers)}
        messages_received_lock = threading.Lock()

        spawned_threads: list[threading.Thread] = self._spawn_queue_consumers(
            number_of_consumers,
            queue_name,
            messages_received,
            messages_received_lock,
        )

        queue_publisher = RabbitMQMessageMiddlewareQueue(
            self._rabbitmq_host(), queue_name
        )
        for _ in range(number_of_consumers):
            queue_publisher.send(message_published)

        for thread in spawned_threads:
            thread.join()

        for i in range(number_of_consumers):
            assert messages_received[f"consumer_{i}"] == message_published

        queue_publisher.delete()
        queue_publisher.close()

    # ============================== TESTS - COMMUNICATION ============================== #

    def test_working_queue_communication_1_to_1(self) -> None:
        self._test_working_queue_communication_1_to(1, "queue-communication-1-to-many")

    def test_working_queue_communication_1_to_many(self) -> None:
        self._test_working_queue_communication_1_to(5, "queue-communication-1-to-many")

    # ============================== TESTS - FUNCTIONS ============================== #

    def test_stop_consuming_multiple_times(self) -> None:
        middleware = RabbitMQMessageMiddlewareQueue(
            self._rabbitmq_host(), "testing-stop-consuming-multiple-times-queue"
        )

        middleware.stop_consuming()
        middleware.stop_consuming()

        middleware.delete()
        middleware.close()

    # ============================== TESTS - EXCEPTIONS ============================== #

    def test_error_while_creating_connection_with_wrong_host(self) -> None:
        with pytest.raises(MessageMiddlewareDisconnectedError) as exc_info:
            RabbitMQMessageMiddlewareQueue("wrong-host", "testing-error-queue")

        assert (
            "Error connecting to RabbitMQ server: [Errno -2] Name or service not known"
            == str(exc_info.value)
        )

    def test_error_while_starting_consuming_from_closed_connection(self) -> None:
        middleware = RabbitMQMessageMiddlewareQueue(
            self._rabbitmq_host(), "testing-error-queue"
        )
        middleware.delete()
        middleware.close()

        with pytest.raises(MessageMiddlewareDisconnectedError) as exc_info:
            middleware.start_consuming(lambda msg: None)

        assert str(exc_info.value) == "Error: Connection or channel is closed."

    def test_consumer_callback_failure_raises_exception(self) -> None:
        middleware = RabbitMQMessageMiddlewareQueue(
            self._rabbitmq_host(), "testing-error-queue"
        )
        middleware.send("message that will cause failure")

        def failing_callback(message_as_bytes: bytes) -> None:
            raise ValueError("Simulated processing error")

        with pytest.raises(MessageMiddlewareMessageError) as exc_info:
            middleware.start_consuming(failing_callback)

        assert (
            str(exc_info.value)
            == "Error consuming messages: Simulated processing error"
        )

        middleware.delete()
        middleware.close()

    def test_error_while_stopping_consuming_from_closed_connection(self) -> None:
        middleware = RabbitMQMessageMiddlewareQueue(
            self._rabbitmq_host(), "testing-error-queue"
        )
        middleware.delete()
        middleware.close()

        with pytest.raises(MessageMiddlewareDisconnectedError) as exc_info:
            middleware.stop_consuming()

        assert str(exc_info.value) == "Error: Connection or channel is closed."

    def test_error_while_sending_msg_to_closed_connection(self) -> None:
        middleware = RabbitMQMessageMiddlewareQueue(
            self._rabbitmq_host(), "testing-error-queue"
        )
        middleware.delete()
        middleware.close()

        with pytest.raises(MessageMiddlewareDisconnectedError) as exc_info:
            middleware.send("Testing message")

        assert str(exc_info.value) == "Error: Connection or channel is closed."

    def test_error_while_sending_wrong_msg(self) -> None:
        middleware = RabbitMQMessageMiddlewareQueue(
            self._rabbitmq_host(), "testing-error-queue"
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
        middleware = RabbitMQMessageMiddlewareQueue(
            self._rabbitmq_host(), "testing-error-queue"
        )
        middleware.delete()
        middleware.close()

        with pytest.raises(MessageMiddlewareCloseError) as exc_info:
            middleware.close()

        assert str(exc_info.value) == "Error closing connection: Channel is closed."

    def test_error_while_deleting_from_closed_connection(self) -> None:
        middleware = RabbitMQMessageMiddlewareQueue(
            self._rabbitmq_host(), "testing-error-queue"
        )
        middleware.delete()
        middleware.close()

        with pytest.raises(MessageMiddlewareDeleteError) as exc_info:
            middleware.delete()

        assert str(exc_info.value) == "Error deleting queue: Channel is closed."
