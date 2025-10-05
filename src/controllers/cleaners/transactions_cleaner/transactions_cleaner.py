import logging
from typing import Any, Callable

from controllers.controller import Controller
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol


class TransactionsCleaner(Controller):

    # ============================== INITIALIZE ============================== #

    def _init_mom_consumers(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> None:
        queue_name_prefix = consumers_config["queue_name_prefix"]
        queue_name = f"{queue_name_prefix}-{self._controller_id}"
        self._mom_consumer = RabbitMQMessageMiddlewareQueue(
            host=rabbitmq_host, queue_name=queue_name
        )

    def _init_mom_producers(
        self,
        rabbitmq_host: str,
        producers_config: dict[str, Any],
    ) -> None:
        self._current_producer_id = 0
        self._mom_producers: list[RabbitMQMessageMiddlewareQueue] = []

        next_controllers_amount = producers_config["next_controllers_amount"]
        for id in range(next_controllers_amount):
            queue_name_prefix = producers_config["queue_name_prefix"]
            queue_name = f"{queue_name_prefix}-{id}"
            mom_producer = RabbitMQMessageMiddlewareQueue(
                host=rabbitmq_host, queue_name=queue_name
            )
            self._mom_producers.append(mom_producer)

    # ============================== PRIVATE - ACCESSING ============================== #

    def _columns_to_keep(self) -> list[str]:
        return [
            "created_at",
            "store_id",
            "final_amount",
            "transaction_id",
            "user_id",
        ]

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _mom_stop_consuming(self) -> None:
        self._mom_consumer.stop_consuming()
        logging.debug("action: sigterm_mom_stop_consuming | result: success")

    # ============================== PRIVATE - FILTER ============================== #

    # @TODO: this is something that can be abstracted to a base cleaner class
    def _filter_item(self, item: dict) -> dict:
        filtered_item = {}
        for column in self._columns_to_keep():
            filtered_item[column] = item[column]
        return filtered_item

    # @TODO: this is something that can be abstracted to a base cleaner class
    def _filter_items_using(
        self, message: str, decoder: Callable, encoder: Callable
    ) -> str:
        batch = []
        for item in decoder(message):
            filtered_item = self._filter_item(item)
            batch.append(filtered_item)
        return str(encoder(batch))

    # @TODO: this is the only should send
    def _filter_message(self, message: str) -> str:
        return self._filter_items_using(
            message,
            communication_protocol.decode_transactions_batch_message,
            communication_protocol.encode_transactions_batch_message,
        )

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_to_next(self, message: str) -> None:
        mom_cleaned_data_producer = self._mom_producers[self._current_producer_id]
        mom_cleaned_data_producer.send(message)

        self._current_producer_id += 1
        if self._current_producer_id >= len(self._mom_producers):
            self._current_producer_id = 0

    def _handle_data_batch_message(self, message: str) -> None:
        filtered_message = self._filter_message(message)
        self._mom_send_message_to_next(filtered_message)

    def _handle_data_batch_eof(self, message: str) -> None:
        for mom_cleaned_data_producer in self._mom_producers:
            mom_cleaned_data_producer.send(message)

    def _handle_received_data(self, message_as_bytes: bytes) -> None:
        if not self._is_running():
            self._mom_consumer.stop_consuming()
            return

        message = message_as_bytes.decode("utf-8")
        logging.debug(
            f"action: message_received | result: success | message: {message}"
        )
        message_type = communication_protocol.decode_message_type(message)
        match message_type:
            case communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE:
                self._handle_data_batch_message(message)
            case communication_protocol.EOF:
                self._handle_data_batch_eof(message)
            case _:
                raise ValueError(f"Invalid message type received: {message_type}")

        # @TODO: this is something that can be abstracted to a base cleaner class

    # ============================== PRIVATE - RUN ============================== #

    def _run(self) -> None:
        super()._run()
        self._mom_consumer.start_consuming(self._handle_received_data)

    def _close_all_mom_connections(self) -> None:
        for mom_producer in self._mom_producers:
            mom_producer.delete()
            mom_producer.close()
            logging.debug("action: mom_producer_producer_close | result: success")

        self._mom_consumer.delete()
        self._mom_consumer.close()
        logging.debug("action: mom_consumer_close | result: success")
