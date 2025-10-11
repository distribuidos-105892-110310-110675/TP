import logging
from abc import abstractmethod
from typing import Any, Callable

from controllers.shared.controller import Controller
from middleware.middleware import MessageMiddleware
from shared import communication_protocol


class Filter(Controller):

    # ============================== INITIALIZE ============================== #

    @abstractmethod
    def _build_mom_consumer_using(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> MessageMiddleware:
        raise NotImplementedError("subclass responsibility")

    def _init_mom_consumers(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> None:
        self._eof_recv_from_prev_controllers = {}
        self._prev_controllers_amount = consumers_config["prev_controllers_amount"]
        self._mom_consumer = self._build_mom_consumer_using(
            rabbitmq_host, consumers_config
        )

    @abstractmethod
    def _build_mom_producer_using(
        self,
        rabbitmq_host: str,
        producers_config: dict[str, Any],
        producer_id: int,
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
        for producer_id in range(next_controllers_amount):
            mom_producer = self._build_mom_producer_using(
                rabbitmq_host, producers_config, producer_id
            )
            self._mom_producers.append(mom_producer)

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _mom_stop_consuming(self) -> None:
        self._mom_consumer.stop_consuming()
        logging.debug("action: sigterm_mom_stop_consuming | result: success")

    # ============================== PRIVATE - TRANSFORM DATA ============================== #

    @abstractmethod
    def _should_be_included(self, batch_item: dict[str, str]) -> bool:
        raise NotImplementedError("subclass responsibility")

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
            if self._should_be_included(item):
                new_batch.append(item)
        return str(encoder(message_type, session_id, new_batch))

    def _transform_batch_message(self, message: str) -> str:
        return self._transform_batch_message_using(
            message,
            communication_protocol.decode_batch_message,
            communication_protocol.encode_batch_message,
            communication_protocol.get_message_type(message),
            communication_protocol.get_message_session_id(message),
        )

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_to_next(self, message: str) -> None:
        mom_producer = self._mom_producers[self._current_producer_id]
        mom_producer.send(message)

        self._current_producer_id += 1
        if self._current_producer_id >= len(self._mom_producers):
            self._current_producer_id = 0

    def _handle_data_batch_message(self, message: str) -> None:
        output_message = self._transform_batch_message(message)
        if not communication_protocol.message_without_payload(output_message):
            self._mom_send_message_to_next(output_message)

    def _handle_data_batch_eof(self, message: str) -> None:
        session_id = communication_protocol.get_message_session_id(message)
        if session_id not in self._eof_recv_from_prev_controllers:
            self._eof_recv_from_prev_controllers[session_id] = 0
        self._eof_recv_from_prev_controllers[session_id] += 1
        logging.debug(f"action: eof_received | result: success")

        if self._eof_recv_from_prev_controllers[session_id] == self._prev_controllers_amount:
            logging.info(f"action: all_eofs_received | session: {session_id} | result: success")
            for mom_producer in self._mom_producers:
                mom_producer.send(message)
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
        for mom_producer in self._mom_producers:
            mom_producer.delete()
            mom_producer.close()
            logging.debug("action: mom_producer_producer_close | result: success")

        self._mom_consumer.delete()
        self._mom_consumer.close()
        logging.debug("action: mom_consumer_close | result: success")
