import logging
from abc import abstractmethod
from typing import Any, Callable

from controllers.shared.controller import Controller
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol


class QueryOutputBuilder(Controller):

    # ============================== INITIALIZE ============================== #

    def _init_mom_consumers(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> None:
        self._eof_recv_from_prev_controllers = {}
        self._prev_controllers_amount = consumers_config["prev_controllers_amount"]

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
        queue_name = producers_config["queue_name_prefix"]
        self._mom_producer = RabbitMQMessageMiddlewareQueue(
            host=rabbitmq_host, queue_name=queue_name
        )

    # ============================== PRIVATE - INTERFACE ============================== #

    @abstractmethod
    def _columns_to_keep(self) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def _output_message_type(self) -> str:
        raise NotImplementedError

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _mom_stop_consuming(self) -> None:
        self._mom_consumer.stop_consuming()
        logging.info("action: sigterm_mom_stop_consuming | result: success")

    # ============================== PRIVATE - TRANSFORM DATA ============================== #

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
        session_id: str,
    ) -> str:
        new_batch = []
        for item in decoder(message):
            modified_item = self._transform_batch_item(item)
            new_batch.append(modified_item)
        return str(encoder(message_type, session_id, new_batch))

    def _transform_batch_message(self, message: str) -> str:
        return self._transform_batch_message_using(
            message,
            communication_protocol.decode_batch_message,
            communication_protocol.encode_batch_message,
            self._output_message_type(),
            communication_protocol.get_message_session_id(message),
        )

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _handle_data_batch_message(self, message: str) -> None:
        output_message = self._transform_batch_message(message)
        self._mom_producer.send(output_message)

    def _handle_data_batch_eof(self, message: str) -> None:
        session_id = communication_protocol.get_message_session_id(message)
        if session_id not in self._eof_recv_from_prev_controllers:
            self._eof_recv_from_prev_controllers[session_id] = 0
        self._eof_recv_from_prev_controllers[session_id] += 1
        logging.debug(f"action: eof_received | session: {session_id} | result: success")

        if self._eof_recv_from_prev_controllers == self._prev_controllers_amount:
            logging.info(f"action: all_eofs_received | session: {session_id} | result: success")
            session_id = communication_protocol.get_message_session_id(message)
            message = communication_protocol.encode_eof_message(
                session_id, self._output_message_type()
            )
            self._mom_producer.send(message)
            logging.info(f"action: eof_sent | session: {session_id} | result: success")

    def _handle_received_data(self, message_as_bytes: bytes) -> None:
        if not self._is_running():
            self._mom_consumer.stop_consuming()
            return

        message = message_as_bytes.decode("utf-8")
        message_type = communication_protocol.get_message_type(message)

        if message_type != communication_protocol.EOF:
            self._handle_data_batch_message(message)
        else:
            self._handle_data_batch_eof(message)

    # ============================== PRIVATE - RUN ============================== #

    def _run(self) -> None:
        super()._run()
        self._mom_consumer.start_consuming(self._handle_received_data)

    def _close_all_mom_connections(self) -> None:
        self._mom_producer.delete()
        self._mom_producer.close()
        logging.debug("action: mom_producer_close | result: success")

        self._mom_consumer.delete()
        self._mom_consumer.close()
        logging.debug("action: mom_consumer_close | result: success")
