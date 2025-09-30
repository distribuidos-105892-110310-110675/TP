import logging
import signal
from typing import Any, Optional, Callable

from shared import communication_protocol

from middleware.rabbitmq_message_middleware_exchange import (
    RabbitMQMessageMiddlewareExchange,
)
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue

ITEMS = [
    {"transaction_id": "ac6f851c-649f-42fb-a606-72be0fdcf8d2", "item_id": "6", "quantity": "1", "unit_price": "9.5", "subtotal": "9.5", "created_at": "2021-01-01 10:06:50"},
    {"transaction_id": "ac6f851c-649f-42fb-a606-72be0fdcf8d2", "item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2022-01-01 10:06:50"},
    {"transaction_id": "ac6f851c-649f-42fb-a606-72be0fdcf8d2", "item_id": "8", "quantity": "3", "unit_price": "10.0", "subtotal": "30.0", "created_at": "2023-01-01 10:06:50"},
    {"transaction_id": "d7856e66-c613-45c4-b9ca-7a3cec6c6db3", "item_id": "2", "quantity": "3", "unit_price": "7.0", "subtotal": "21.0", "created_at": "2024-01-01 10:06:52"},
    {"transaction_id": "d7856e66-c613-45c4-b9ca-7a3cec6c6db3", "item_id": "6", "quantity": "1", "unit_price": "9.5", "subtotal": "9.5", "created_at": "2025-01-01 10:06:52"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "1", "quantity": "1", "unit_price": "6.0", "subtotal": "6.0", "created_at": "2021-01-01 10:06:53"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-01-01 10:06:53"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2025-01-01 10:06:53"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "7", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2022-01-01 10:06:53"},
]

class FilterItemsByYear:
    # ============================== INITIALIZE ============================== #

    def __init_mom_consumer(self, host: str, data_queue_prefix: str) -> None:
        queue_name = f"{data_queue_prefix}-{self._controller_id}"
        self._mom_consumer = RabbitMQMessageMiddlewareQueue(
            host=host, queue_name=queue_name
        )

    def __init_mom_producers(
            self,
            host: str,
            filtered_items_queue_prefix: str, filtered_items_queues_amount: int
    ) -> None:
        self._current_filtered_items_producer_id = 0
        self._mom_producers: list[RabbitMQMessageMiddlewareQueue] = []
        for id in range(filtered_items_queues_amount):
            queue_name = f"{filtered_items_queue_prefix}-{id}"
            mom_cleaned_data_producer = RabbitMQMessageMiddlewareQueue(
                host=host, queue_name=queue_name
            )
            self._mom_producers.append(mom_cleaned_data_producer)

    def __init__(
            self,
            controller_id: int,
            rabbitmq_host: str,
            consumer_queue_prefix: str,
            producer_queue_prefix: str,
            previous_controllers_amount: int,
            next_controllers_amount: int,
            years_to_filter: list[int],
    ) -> None:
        self._controller_id = controller_id

        self.__set_controller_as_not_running()
        signal.signal(signal.SIGTERM, self.__sigterm_signal_handler)

        self.__init_mom_consumer(
            rabbitmq_host,
            consumer_queue_prefix,
        )
        self.__init_mom_producers(
            rabbitmq_host,
            producer_queue_prefix,
            next_controllers_amount,
        )

        self._eof_received_from_previous_controllers = 0
        self._previous_controllers_amount = previous_controllers_amount

        self._years_to_filter = set(years_to_filter)

    # ============================== PRIVATE - ACCESSING ============================== #

    def __is_running(self) -> bool:
        return self._controller_running

    def __set_controller_as_not_running(self) -> None:
        self._controller_running = False

    def __set_controller_as_running(self) -> None:
        self._controller_running = True

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def __sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        logging.info("action: sigterm_signal_handler | result: in_progress")

        self.__set_controller_as_not_running()

        self._mom_consumer.stop_consuming()
        logging.debug("action: sigterm_mom_stop_consuming | result: success")

        logging.info("action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - TRANSFORM DATA ============================== #

    def __transform_batch_item(self, batch_item: dict[str, str]) -> Optional[dict]:
        created_at = batch_item["created_at"]
        date = created_at.split(" ")[0]
        year = int(date.split("-")[0])
        if year not in self._years_to_filter:
            return None
        return batch_item

    def __transform_batch_message_using(
            self,
            message: str,
            decoder: Callable,
            encoder: Callable,
            output_message_type: Optional[str] = None,
    ) -> str:
        message_type = output_message_type
        if output_message_type is None:
            message_type = communication_protocol.decode_message_type(message)

        new_batch = []
        for item in decoder(message):
            modified_item = self.__transform_batch_item(item)
            if modified_item is not None:
                new_batch.append(modified_item)
        return str(encoder(message_type, new_batch))

    def __transform_batch_message(self, message: str) -> str:
        return self.__transform_batch_message_using(
            message,
            communication_protocol.decode_batch_message,
            communication_protocol.encode_batch_message,
        )

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def __mom_send_message_to_next(self, message: str) -> None:
        mom_filtered_items_producer = self._mom_producers[
            self._current_filtered_items_producer_id
        ]
        mom_filtered_items_producer.send(message)

        self._current_filtered_items_producer_id += 1
        if self._current_filtered_items_producer_id >= len(
                self._mom_producers
        ):
            self._current_filtered_items_producer_id = 0

    def __handle_data_batch_message(self, message: str) -> None:
        output_message = self.__transform_batch_message(message)
        if not communication_protocol.decode_is_empty_message(output_message):
            self.__mom_send_message_to_next(output_message)

    def __handle_data_batch_eof(self, message: str) -> None:
        self._eof_received_from_previous_controllers += 1
        logging.debug(f"action: eof_received | result: success")

        if (
                self._eof_received_from_previous_controllers
                == self._previous_controllers_amount
        ):
            logging.info("action: all_eofs_received | result: success")
            for mom_producer in self._mom_producers:
                mom_producer.send(message)
            logging.info("action: eof_sent | result: success")

    def __handle_received_data(self, message_as_bytes: bytes) -> None:
        if not self.__is_running():
            self._mom_consumer.stop_consuming()
            return

        message = message_as_bytes.decode("utf-8")
        message_type = communication_protocol.decode_message_type(message)

        if message_type != communication_protocol.EOF:
            self.__handle_data_batch_message(message)
        else:
            self.__handle_data_batch_eof(message)

    # ============================== PRIVATE - RUN ============================== #

    def __run(self) -> None:
        self.__set_controller_as_running()
        self._mom_consumer.start_consuming(self.__handle_received_data)

    def __close_all_mom_connections(self) -> None:
        for mom_producer in self._mom_producers:
            mom_producer.delete()
            mom_producer.close()
            logging.debug("action: mom_producer_close | result: success")

        self._mom_consumer.delete()
        self._mom_consumer.close()
        logging.debug("action: mom_consumer_close | result: success")

    def __ensure_connections_close_after_doing(self, callback: Callable) -> None:
        try:
            callback()
        except Exception as e:
            logging.error(f"action: controller_run | result: fail | error: {e}")
            raise e
        finally:
            self.__close_all_mom_connections()
            logging.debug("action: all_mom_connections_close | result: success")

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        logging.info("action: controller_startup | result: success")

        self.__ensure_connections_close_after_doing(self.__run)

        logging.info("action: controller_shutdown | result: success")