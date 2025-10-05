import logging
import signal
from typing import Any, Callable

from middleware.rabbitmq_message_middleware_exchange import (
    RabbitMQMessageMiddlewareExchange,
)
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol


class JoinTransactionsWithStores:

    def __init_mom_base_data_consumer(
        self,
        host: str,
        consumer_exchange_prefix: str,
        consumer_routing_key_prefix: str,
    ) -> None:
        exchange_name = consumer_exchange_prefix
        routing_keys = [f"{consumer_routing_key_prefix}.*"]
        self._mom_base_data_consumer = RabbitMQMessageMiddlewareExchange(
            host=host,
            exchange_name=exchange_name,
            route_keys=routing_keys,
        )

    def __init_mom_stream_data_consumer(
        self,
        host: str,
        consumer_queue_prefix: str,
    ) -> None:
        queue_name = f"{consumer_queue_prefix}-{self._controller_id}"
        self._mom_stream_data_consumer = RabbitMQMessageMiddlewareQueue(
            host=host, queue_name=queue_name
        )

    def __init_mom_producers(
        self,
        host: str,
        producer_queue_prefix: str,
        producer_queue_amount: int,
    ) -> None:
        self._current_producer_id = 0
        self._mom_producers: list[RabbitMQMessageMiddlewareQueue] = []
        for id in range(producer_queue_amount):
            queue_name = f"{producer_queue_prefix}-{id}"
            mom_producer = RabbitMQMessageMiddlewareQueue(
                host=host, queue_name=queue_name
            )
            self._mom_producers.append(mom_producer)

    def __init__(
        self,
        controller_id: int,
        rabbitmq_host: str,
        base_data_consumer_exchange_prefix: str,
        base_data_consumer_routing_key_prefix: str,
        stream_consumer_queue_prefix: str,
        producer_queue_prefix: str,
        previos_base_data_controllers_amount: int,
        previos_stream_data_controllers_amount: int,
        next_controllers_amount: int,
    ) -> None:
        self._controller_id = controller_id

        self._set_controller_as_not_running()
        signal.signal(signal.SIGTERM, self._sigterm_signal_handler)

        self.__init_mom_base_data_consumer(
            rabbitmq_host,
            base_data_consumer_exchange_prefix,
            base_data_consumer_routing_key_prefix,
        )
        self.__init_mom_stream_data_consumer(
            rabbitmq_host,
            stream_consumer_queue_prefix,
        )
        self.__init_mom_producers(
            rabbitmq_host,
            producer_queue_prefix,
            next_controllers_amount,
        )

        self._eof_received_from_base_data_controllers = 0
        self._eof_received_from_stream_data_controllers = 0
        self._previous_base_data_controllers_amount = (
            previos_base_data_controllers_amount
        )
        self._previous_stream_data_controllers_amount = (
            previos_stream_data_controllers_amount
        )

        self._base_data: list[dict[str, str]] = []

    # ============================== PRIVATE - ACCESSING ============================== #

    def _is_running(self) -> bool:
        return self._server_running

    def _set_controller_as_not_running(self) -> None:
        self._server_running = False

    def _set_controller_as_running(self) -> None:
        self._server_running = True

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        logging.info("action: sigterm_signal_handler | result: in_progress")

        self._set_controller_as_not_running()

        self._mom_base_data_consumer.stop_consuming()
        self._mom_stream_data_consumer.stop_consuming()
        logging.debug("action: sigterm_mom_stop_consuming | result: success")

        logging.info("action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - JOIN ============================== #

    def _join_with_base_data(self, message: str) -> str:
        message_type = communication_protocol.decode_message_type(message)
        stream_data = communication_protocol.decode_batch_message(message)
        joined_data: list[dict[str, str]] = []
        for stream_item in stream_data:
            was_joined = False
            stream_store_id = int(float(stream_item["store_id"]))
            for base_item in self._base_data:
                base_store_id = int(float(base_item["store_id"]))
                if base_store_id == stream_store_id:
                    joined_item = {**stream_item, **base_item}
                    joined_data.append(joined_item)
                    was_joined = True
                    break
            if not was_joined:
                logging.warning(
                    f"action: join_failed | store_id: {stream_item['store_id']} | result: skipped"
                )
        return communication_protocol.encode_batch_message(message_type, joined_data)

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
        self._eof_received_from_base_data_controllers += 1
        logging.debug(f"action: eof_received | result: success")

        if (
            self._eof_received_from_base_data_controllers
            == self._previous_base_data_controllers_amount
        ):
            logging.info("action: all_eofs_received | result: success")
            self._mom_base_data_consumer.stop_consuming()
            logging.info("action: stop_consuming_base_data | result: success")

    def _handle_base_data(self, message_as_bytes: bytes) -> None:
        if not self._is_running():
            self._mom_base_data_consumer.stop_consuming()
            return

        message = message_as_bytes.decode("utf-8")
        message_type = communication_protocol.decode_message_type(message)

        if message_type != communication_protocol.EOF:
            self._handle_base_data_batch_message(message)
        else:
            self._handle_base_data_batch_eof(message)

    def _handle_stream_data_batch_message(self, message: str) -> None:
        joined_message = self._join_with_base_data(message)
        if not communication_protocol.decode_is_empty_message(joined_message):
            self._mom_send_message_to_next(joined_message)

    def _handle_stream_data_batch_eof(self, message: str) -> None:
        self._eof_received_from_stream_data_controllers += 1
        logging.debug(f"action: eof_received | result: success")

        if (
            self._eof_received_from_stream_data_controllers
            == self._previous_stream_data_controllers_amount
        ):
            logging.info("action: all_eofs_received | result: success")
            for mom_producer in self._mom_producers:
                mom_producer.send(message)
            logging.info("action: eof_sent | result: success")

    def _handle_stream_data(self, message_as_bytes: bytes) -> None:
        if not self._is_running():
            self._mom_stream_data_consumer.stop_consuming()
            return

        message = message_as_bytes.decode("utf-8")
        message_type = communication_protocol.decode_message_type(message)

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

    def _ensure_connections_close_after_doing(self, callback: Callable) -> None:
        try:
            callback()
        except Exception as e:
            logging.error(f"action: cleaner_run | result: fail | error: {e}")
            raise e
        finally:
            self._close_all_mom_connections()
            logging.debug("action: all_mom_connections_close | result: success")

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        logging.info("action: cleaner_startup | result: success")

        self._ensure_connections_close_after_doing(self._run)

        logging.info("action: cleaner_shutdown | result: success")
