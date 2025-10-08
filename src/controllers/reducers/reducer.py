import logging
from abc import abstractmethod
from typing import Any

from controllers.controller import Controller
from middleware.middleware import MessageMiddleware
from shared import communication_protocol


class Reducer(Controller):

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
        self._eof_recv_from_prev_controllers = 0
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

    def __init__(
        self,
        controller_id: int,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
        producers_config: dict[str, Any],
        batch_max_size: int,
    ) -> None:
        super().__init__(
            controller_id,
            rabbitmq_host,
            consumers_config,
            producers_config,
        )

        self._batch_max_size = batch_max_size

        self._reduced_data: dict[tuple, float] = {}

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _mom_stop_consuming(self) -> None:
        self._mom_consumer.stop_consuming()
        logging.debug("action: sigterm_mom_stop_consuming | result: success")

    # ============================== PRIVATE - ACCESSING ============================== #

    @abstractmethod
    def _keys(self) -> list[str]:
        raise NotImplementedError("subclass responsibility")

    @abstractmethod
    def _accumulator_name(self) -> str:
        raise NotImplementedError("subclass responsibility")

    # ============================== PRIVATE - HANDLE DATA ============================== #

    @abstractmethod
    def _reduce_function(
        self, current_value: float, batch_item: dict[str, str]
    ) -> float:
        raise NotImplementedError("subclass responsibility")

    def _reduce_by_keys(self, batch_item: dict[str, str]) -> None:
        for k in self._keys():
            if batch_item[k] == "":
                logging.warning(f"action: empty_{k} | result: skipped")
                return

        key = tuple(batch_item[k] for k in self._keys())
        if key not in self._reduced_data:
            self._reduced_data[key] = 0

        self._reduced_data[key] = self._reduce_function(
            self._reduced_data[key], batch_item
        )

    def _pop_next_batch_item(self) -> dict[str, str]:
        key, value = self._reduced_data.popitem()
        batch_item: dict[str, str] = {}
        for i, k in enumerate(self._keys()):
            batch_item[k] = key[i]
        batch_item[self._accumulator_name()] = str(value)
        return batch_item

    def _take_next_batch(self) -> list[dict[str, str]]:
        batch: list[dict[str, str]] = []

        all_batchs_taken = False
        while not all_batchs_taken and len(batch) < self._batch_max_size:
            if not self._reduced_data:
                all_batchs_taken = True
                break

            batch_item = self._pop_next_batch_item()
            batch.append(batch_item)

        return batch

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_to_next(self, message: str) -> None:
        mom_cleaned_data_producer = self._mom_producers[self._current_producer_id]
        mom_cleaned_data_producer.send(message)

        self._current_producer_id += 1
        if self._current_producer_id >= len(self._mom_producers):
            self._current_producer_id = 0

    def _send_all_data_using_batchs(self) -> None:
        batch = self._take_next_batch()
        while len(batch) != 0 and self._is_running():
            message = communication_protocol.encode_transactions_batch_message(batch)
            self._mom_send_message_to_next(message)
            batch = self._take_next_batch()

    def _handle_data_batch_message(self, message: str) -> None:
        batch = communication_protocol.decode_batch_message(message)
        for batch_item in batch:
            self._reduce_by_keys(batch_item)

    def _handle_data_batch_eof(self, message: str) -> None:
        self._eof_recv_from_prev_controllers += 1
        logging.debug(f"action: eof_received | result: success")

        if self._eof_recv_from_prev_controllers == self._prev_controllers_amount:
            logging.info("action: all_eofs_received | result: success")

            self._send_all_data_using_batchs()
            logging.info("action: all_data_sent | result: success")

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
