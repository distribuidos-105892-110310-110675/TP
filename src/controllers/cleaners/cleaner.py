import logging
from abc import abstractmethod
from typing import Any, Callable, Optional

from controllers.controller import Controller
from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol


class Cleaner(Controller):

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

    @abstractmethod
    def _build_mom_producer_using(
        self, rabbitmq_host: str, producers_config: dict[str, Any], producer_id: int
    ) -> MessageMiddleware:
        raise NotImplementedError("subclass responsibility")

    def _init_mom_producers(
        self,
        rabbitmq_host: str,
        producers_config: dict[str, Any],
    ) -> None:
        self._current_producer_id = 0
        self._mom_producers: list[MessageMiddleware] = []

        next_controllers_amount = producers_config["next_controllers_amount"]
        for id in range(next_controllers_amount):
            mom_producer = self._build_mom_producer_using(
                rabbitmq_host, producers_config, id
            )
            self._mom_producers.append(mom_producer)

    # ============================== PRIVATE - ACCESSING ============================== #

    @abstractmethod
    def _columns_to_keep(self) -> list[str]:
        raise NotImplementedError("subclass responsibility")

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _mom_stop_consuming(self) -> None:
        self._mom_consumer.stop_consuming()
        logging.debug("action: sigterm_mom_stop_consuming | result: success")

    # ============================== PRIVATE - FILTER ============================== #

    def _transform_batch_item(self, batch_item: dict[str, str]) -> dict:
        modified_item_batch = {}
        for column in self._columns_to_keep():
            modified_item_batch[column] = batch_item[column]
        return modified_item_batch

    def _transform_batch_message_using(
        self,
        message: str,
        decoder: Callable,
        encoder: Callable,
        message_type: str,
    ) -> str:
        new_batch = []
        for item in decoder(message):
            modified_item = self._transform_batch_item(item)
            new_batch.append(modified_item)
        return str(encoder(message_type, new_batch))

    def _transform_batch_message(self, message: str) -> str:
        return self._transform_batch_message_using(
            message,
            communication_protocol.decode_batch_message,
            communication_protocol.encode_batch_message,
            communication_protocol.decode_message_type(message),
        )

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    @abstractmethod
    def _mom_send_message_to_next(self, message: str) -> None:
        raise NotImplementedError("subclass responsibility")

    def _handle_data_batch_message(self, message: str) -> None:
        filtered_message = self._transform_batch_message(message)
        self._mom_send_message_to_next(filtered_message)

    def _handle_data_batch_eof(self, message: str) -> None:
        logging.debug(f"action: eof_received | result: success")

        for mom_producer in self._mom_producers:
            mom_producer.send(message)

        logging.info("action: eof_sent | result: success")

    def _handle_received_data(self, message_as_bytes: bytes) -> None:
        if not self._is_running():
            self._mom_consumer.stop_consuming()
            return

        message = message_as_bytes.decode("utf-8")
        message_type = communication_protocol.decode_message_type(message)
        if message_type != communication_protocol.EOF:
            self._handle_data_batch_message(message)
        else:
            self._handle_data_batch_eof(message)

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
