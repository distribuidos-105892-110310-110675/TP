import logging
from abc import abstractmethod
from typing import Any

from controllers.controller import Controller
from middleware.middleware import MessageMiddleware
from shared import communication_protocol


class Joiner(Controller):

    @abstractmethod
    def _build_mom_base_data_consumer(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> MessageMiddleware:
        raise NotImplementedError("subclass responsibility")

    @abstractmethod
    def _build_mom_stream_data_consumer(
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
        self._eof_recv_from_base_data_prev_controllers = {}
        self._base_data_prev_controllers_amount = consumers_config[
            "base_data_prev_controllers_amount"
        ]

        self._eof_recv_from_stream_data_prev_controllers = {}
        self._stream_data_prev_controllers_amount = consumers_config[
            "stream_data_prev_controllers_amount"
        ]

        self._mom_base_data_consumer = self._build_mom_base_data_consumer(
            rabbitmq_host, consumers_config
        )
        self._mom_stream_data_consumer = self._build_mom_stream_data_consumer(
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

    def __init__(
        self,
        controller_id: int,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
        producers_config: dict[str, Any],
    ) -> None:
        super().__init__(
            controller_id,
            rabbitmq_host,
            consumers_config,
            producers_config,
        )

        self._base_data: list[dict[str, str]] = []

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _mom_stop_consuming(self) -> None:
        self._mom_base_data_consumer.stop_consuming()
        self._mom_stream_data_consumer.stop_consuming()
        logging.debug("action: sigterm_mom_stop_consuming | result: success")

    # ============================== PRIVATE - ACCESSING ============================== #

    @abstractmethod
    def _join_key(self) -> str:
        raise NotImplementedError("subclass responsibility")

    @abstractmethod
    def _transform_function(self, value: str) -> Any:
        raise NotImplementedError("subclass responsibility")

    # ============================== PRIVATE - JOIN ============================== #

    def _should_be_joined(
        self, base_item: dict[str, str], stream_item: dict[str, str]
    ) -> bool:
        base_optional_value = base_item.get(self._join_key())
        stream_optional_value = stream_item.get(self._join_key())
        if base_optional_value is None or stream_optional_value is None:
            return False

        base_value = self._transform_function(base_optional_value)
        stream_value = self._transform_function(stream_optional_value)
        return base_value == stream_value

    def _join_with_base_data(self, message: str) -> str:
        message_type = communication_protocol.get_message_type(message)
        session_id = communication_protocol.get_message_session_id(message)
        stream_data = communication_protocol.decode_batch_message(message)
        joined_data: list[dict[str, str]] = []
        for stream_item in stream_data:
            was_joined = False
            for base_item in self._base_data:
                if self._should_be_joined(base_item, stream_item):
                    joined_item = {**stream_item, **base_item}
                    joined_data.append(joined_item)
                    was_joined = True
                    break
            if not was_joined:
                logging.warning(
                    f"action: join_with_base_data | result: error | stream_item: {stream_item}"
                )
        return communication_protocol.encode_batch_message(
            message_type, session_id, joined_data
        )

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_to_next(self, message: str) -> None:
        mom_cleaned_data_producer = self._mom_producers[self._current_producer_id]
        mom_cleaned_data_producer.send(message)

        self._current_producer_id += 1
        if self._current_producer_id >= len(self._mom_producers):
            self._current_producer_id = 0

    def _handle_base_data_batch_message(self, message: str) -> None:
        batch_message = communication_protocol.decode_batch_message(message)
        for item_batch in batch_message:
            self._base_data.append(item_batch)

    def _handle_base_data_batch_eof(self, message: str) -> None:
        session_id = communication_protocol.get_message_session_id(message)
        if session_id not in self._eof_recv_from_base_data_prev_controllers:
            self._eof_recv_from_base_data_prev_controllers[session_id] = 0
        self._eof_recv_from_base_data_prev_controllers[session_id] += 1
        logging.debug(f"action: eof_received | session: {session_id} | result: success")

        if (
            self._eof_recv_from_base_data_prev_controllers[session_id]
            == self._base_data_prev_controllers_amount
        ):
            logging.info(f"action: all_eofs_received | session: {session_id} | result: success")
            self._mom_base_data_consumer.stop_consuming()
            logging.info("action: stop_consuming_base_data | result: success")

    def _handle_base_data(self, message_as_bytes: bytes) -> None:
        if not self._is_running():
            self._mom_base_data_consumer.stop_consuming()
            return

        message = message_as_bytes.decode("utf-8")
        message_type = communication_protocol.get_message_type(message)

        if message_type != communication_protocol.EOF:
            self._handle_base_data_batch_message(message)
        else:
            self._handle_base_data_batch_eof(message)

    def _handle_stream_data_batch_message(self, message: str) -> None:
        joined_message = self._join_with_base_data(message)
        if not communication_protocol.message_without_payload(joined_message):
            self._mom_send_message_to_next(joined_message)

    def _handle_stream_data_batch_eof(self, message: str) -> None:
        session_id = communication_protocol.get_message_session_id(message)
        if session_id not in self._eof_recv_from_stream_data_prev_controllers:
            self._eof_recv_from_stream_data_prev_controllers[session_id] = 0
        self._eof_recv_from_stream_data_prev_controllers[session_id] += 1
        logging.debug(f"action: eof_received | session: {session_id} | result: success")

        if (
            self._eof_recv_from_stream_data_prev_controllers[session_id]
            == self._stream_data_prev_controllers_amount
        ):
            logging.info(f"action: all_eofs_received | session: {session_id} | result: success")
            for mom_producer in self._mom_producers:
                mom_producer.send(message)
            logging.info(f"action: eof_sent | session: {session_id} | result: success")

            self._base_data.clear()

    def _handle_stream_data(self, message_as_bytes: bytes) -> None:
        if not self._is_running():
            self._mom_stream_data_consumer.stop_consuming()
            return

        message = message_as_bytes.decode("utf-8")
        message_type = communication_protocol.get_message_type(message)

        if message_type != communication_protocol.EOF:
            self._handle_stream_data_batch_message(message)
        else:
            self._handle_stream_data_batch_eof(message)

    # ============================== PRIVATE - RUN ============================== #

    def _run(self) -> None:
        self._set_controller_as_running()
        self._mom_base_data_consumer.start_consuming(self._handle_base_data)
        self._mom_stream_data_consumer.start_consuming(self._handle_stream_data)

    def _close_all_mom_connections(self) -> None:
        for mom_producer in self._mom_producers:
            mom_producer.delete()
            mom_producer.close()
            logging.debug("action: mom_cleaned_data_producer_close | result: success")

        self._mom_base_data_consumer.delete()
        self._mom_base_data_consumer.close()
        self._mom_stream_data_consumer.delete()
        self._mom_stream_data_consumer.close()
        logging.debug("action: mom_data_consumer_close | result: success")
