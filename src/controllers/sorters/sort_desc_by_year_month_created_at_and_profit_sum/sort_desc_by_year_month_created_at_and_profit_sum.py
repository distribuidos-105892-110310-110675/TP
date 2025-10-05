import logging
import signal
from typing import Any, Callable

from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol


class SortDescByYearMonthCreatedAtAndProfitSum:

    # ============================== INITIALIZE ============================== #

    def __init_mom_consumer(
        self,
        host: str,
        queue_prefix: str,
    ) -> None:
        queue_name = f"{queue_prefix}-{self._controller_id}"
        self._mom_consumer = RabbitMQMessageMiddlewareQueue(
            host=host, queue_name=queue_name
        )

    def __init_mom_producers(
        self,
        host: str,
        producer_queue_prefix: str,
        next_controllers_amount: int,
    ) -> None:
        self._current_producer_id = 0
        self._mom_producers: list[RabbitMQMessageMiddlewareQueue] = []
        for i in range(next_controllers_amount):
            queue_name = f"{producer_queue_prefix}-{i}"
            self._mom_producers.append(
                RabbitMQMessageMiddlewareQueue(
                    host=host,
                    queue_name=queue_name,
                )
            )

    def __init__(
        self,
        controller_id: int,
        rabbitmq_host: str,
        consumer_queue_prefix: str,
        producer_queue_prefix: str,
        previous_controllers_amount: int,
        next_controllers_amount: int,
        batch_max_size: int,
        amount_per_group: int,
    ) -> None:
        self._controller_id = controller_id

        self._set_controller_as_not_running()
        signal.signal(signal.SIGTERM, self._sigterm_signal_handler)

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

        self._batch_max_size = batch_max_size
        self._amount_per_group = amount_per_group

        self._grouping_key = "year_month_created_at"
        self._primary_sort_key = "year_month_created_at"
        self._secondary_sort_key = "profit_sum"

        self._sorted_desc_by_grouping_key: dict[str, list[dict[str, str]]] = {}

    # ============================== PRIVATE - ACCESSING ============================== #

    def _is_running(self) -> bool:
        return self._controller_running

    def _set_controller_as_not_running(self) -> None:
        self._controller_running = False

    def _set_controller_as_running(self) -> None:
        self._controller_running = True

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        logging.info("action: sigterm_signal_handler | result: in_progress")

        self._set_controller_as_not_running()

        self._mom_consumer.stop_consuming()
        logging.debug("action: sigterm_mom_stop_consuming | result: success")

        logging.info("action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - HANDLE DATA ============================== #

    def _sort_desc_by_purchases_qty(self, batch_item: dict[str, str]) -> None:
        grouping_key_value = batch_item[self._grouping_key]
        primary_sort_value = batch_item[self._primary_sort_key]
        secondary_sort_value = batch_item[self._secondary_sort_key]

        if grouping_key_value not in self._sorted_desc_by_grouping_key:
            self._sorted_desc_by_grouping_key[grouping_key_value] = []
        sorted_desc_batch_items = self._sorted_desc_by_grouping_key[grouping_key_value]

        index = 0
        while index < len(sorted_desc_batch_items):
            current_batch_item = sorted_desc_batch_items[index]
            current_primary_sort_value = current_batch_item[self._primary_sort_key]
            current_secondary_sort_value = current_batch_item[self._secondary_sort_key]

            if primary_sort_value > current_primary_sort_value:
                break
            if primary_sort_value == current_primary_sort_value:
                if secondary_sort_value > current_secondary_sort_value:
                    break
            index += 1

        sorted_desc_batch_items.insert(index, batch_item)
        if len(sorted_desc_batch_items) > self._amount_per_group:
            sorted_desc_batch_items.pop()

    def _pop_next_batch_item(self) -> dict[str, str]:
        key = next(iter(self._sorted_desc_by_grouping_key))
        batch_item = self._sorted_desc_by_grouping_key[key].pop(0)
        if not self._sorted_desc_by_grouping_key[key]:
            del self._sorted_desc_by_grouping_key[key]
        return batch_item

    def _take_next_batch(self) -> list[dict[str, str]]:
        batch: list[dict[str, str]] = []

        batch_size = 0
        all_batchs_taken = False

        while not all_batchs_taken and batch_size < self._batch_max_size:
            if not self._sorted_desc_by_grouping_key:
                all_batchs_taken = True
                break

            batch_item = self._pop_next_batch_item()
            batch.append(batch_item)
            batch_size += 1

        return batch

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _send_data_using_batchs(
        self, mom_producer: RabbitMQMessageMiddlewareQueue
    ) -> None:
        batch = self._take_next_batch()
        while len(batch) != 0 and self._is_running():
            message = communication_protocol.encode_transactions_batch_message(batch)
            mom_producer.send(message)
            logging.debug(
                f"action: message_sent | result: success | message: {message}"
            )
            batch = self._take_next_batch()

    def _handle_data_batch_message(self, message: str) -> None:
        batch = communication_protocol.decode_batch_message(message)
        for batch_item in batch:
            self._sort_desc_by_purchases_qty(batch_item)

    def _handle_data_batch_eof(self, message: str) -> None:
        self._eof_received_from_previous_controllers += 1
        logging.debug(f"action: eof_received | result: success")

        if (
            self._eof_received_from_previous_controllers
            == self._previous_controllers_amount
        ):
            logging.info("action: all_eofs_received | result: success")
            for mom_producer in self._mom_producers:
                self._send_data_using_batchs(mom_producer)

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
        self._set_controller_as_running()
        self._mom_consumer.start_consuming(self._handle_received_data)

    def _close_all_mom_connections(self) -> None:
        for mom_producer in self._mom_producers:
            mom_producer.delete()
            mom_producer.close()
            logging.debug("action: mom_producer_close | result: success")

        self._mom_consumer.delete()
        self._mom_consumer.close()
        logging.debug("action: mom_consumer_close | result: success")

    def _ensure_connections_close_after_doing(self, callback: Callable) -> None:
        try:
            callback()
        except Exception as e:
            logging.error(f"action: controller_run | result: fail | error: {e}")
            raise e
        finally:
            self._close_all_mom_connections()
            logging.debug("action: all_mom_connections_close | result: success")

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        logging.info("action: controller_startup | result: success")

        self._ensure_connections_close_after_doing(self._run)

        logging.info("action: controller_shutdown | result: success")
