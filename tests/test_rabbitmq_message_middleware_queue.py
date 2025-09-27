import threading

from middleware.middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
)
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue


class TestRabbitMQMessageMiddlewareQueue:

    # ============================== PRIVATE - ACCESSING ============================== #

    def __rabbitmq_host(self) -> str:
        return "rabbitmq-dev"

    # ============================== PRIVATE - TESTS SUPPORT ============================== #

    def __consumer_handler_with_id(
        self,
        consumer_id: int,
        queue_name: str,
        messages_received: dict,
        message_received_lock: threading.Lock,
    ) -> None:
        queue_consumer = RabbitMQMessageMiddlewareQueue(
            self.__rabbitmq_host(), queue_name
        )

        def on_message_callback(message_as_bytes: bytes) -> None:
            nonlocal messages_received
            message_received = message_as_bytes.decode("utf-8")

            with message_received_lock:
                messages_received[f"consumer_{consumer_id}"] = message_received

            queue_consumer.stop_consuming()

        queue_consumer.start_consuming(on_message_callback)

        queue_consumer.close()

    def __spawn_queue_consumers(
        self,
        number_of_consumers: int,
        queue_name: str,
        messages_received: dict,
        message_received_lock: threading.Lock,
    ) -> list[threading.Thread]:
        spawned_threads: list[threading.Thread] = []

        for i in range(number_of_consumers):
            thread = threading.Thread(
                target=self.__consumer_handler_with_id,
                args=(i, queue_name, messages_received, message_received_lock),
            )
            thread.start()
            spawned_threads.append(thread)

        return spawned_threads

    def __test_working_queue_communication_1_to(
        self, number_of_consumers: int, queue_name: str
    ) -> None:
        message_published = "Testing message"

        messages_received = {f"consumer_{i}": "" for i in range(number_of_consumers)}
        messages_received_lock = threading.Lock()

        spawned_threads: list[threading.Thread] = self.__spawn_queue_consumers(
            number_of_consumers,
            queue_name,
            messages_received,
            messages_received_lock,
        )

        queue_publisher = RabbitMQMessageMiddlewareQueue(
            self.__rabbitmq_host(), queue_name
        )
        for _ in range(number_of_consumers):
            queue_publisher.send(message_published)

        for thread in spawned_threads:
            thread.join()

        for i in range(number_of_consumers):
            assert messages_received[f"consumer_{i}"] == message_published

        queue_publisher.delete()
        queue_publisher.close()

    # ============================== TESTS ============================== #

    def test_working_queue_communication_1_to_1(self) -> None:
        self.__test_working_queue_communication_1_to(1, "queue-communication-1-to-many")

    def test_working_queue_communication_1_to_many(self) -> None:
        self.__test_working_queue_communication_1_to(5, "queue-communication-1-to-many")

    # try to make tests for the other exceptions
    # on the other methods

    def test_error_while_closing_already_closed_connection(self) -> None:
        middleware = RabbitMQMessageMiddlewareQueue(
            self.__rabbitmq_host(), "queue-error-while-closing-already-closed"
        )
        middleware.delete()
        middleware.close()

        raise_right_exception = False
        try:
            middleware.close()
        except MessageMiddlewareCloseError as e:
            raise_right_exception = True
            assert str(e) == "Error closing connection: Channel is closed."

        assert raise_right_exception

    def test_error_while_deleting_from_already_closed_connection(self) -> None:
        middleware = RabbitMQMessageMiddlewareQueue(
            self.__rabbitmq_host(), "queue-error-while-deleting-already-deleted"
        )
        middleware.delete()
        middleware.close()

        raise_right_exception = False
        try:
            middleware.delete()
        except MessageMiddlewareDeleteError as e:
            raise_right_exception = True
            assert str(e) == "Error deleting queue: Channel is closed."

        assert raise_right_exception
