import logging
import signal
from typing import Any, Callable

from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol


class SortDescByStoreIdAndPurchasesQty:

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
        self._number_of_customers_per_store = amount_per_group

        self._sorted_desc_by_store_id_and_purchases_qty: dict[
            str, list[dict[str, str]]
        ] = {}

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
        store_id = batch_item["store_id"]
        purchases_qty = float(batch_item["purchases_qty"])

        if store_id not in self._sorted_desc_by_store_id_and_purchases_qty:
            self._sorted_desc_by_store_id_and_purchases_qty[store_id] = []
        sorted_desc_by_purchases_qty = self._sorted_desc_by_store_id_and_purchases_qty[
            store_id
        ]

        index = 0
        while index < len(sorted_desc_by_purchases_qty):
            current_item = sorted_desc_by_purchases_qty[index]
            current_store_id = current_item["store_id"]
            current_purchases_qty = float(current_item["purchases_qty"])

            if store_id > current_store_id:
                break
            if store_id == current_store_id:
                if purchases_qty > current_purchases_qty:
                    break
            index += 1

        sorted_desc_by_purchases_qty.insert(index, batch_item)
        if len(sorted_desc_by_purchases_qty) > self._number_of_customers_per_store:
            sorted_desc_by_purchases_qty.pop()

    def _pop_next_batch_item(self) -> dict[str, str]:
        store_id = next(iter(self._sorted_desc_by_store_id_and_purchases_qty))
        batch_item = self._sorted_desc_by_store_id_and_purchases_qty[store_id].pop(0)
        if not self._sorted_desc_by_store_id_and_purchases_qty[store_id]:
            del self._sorted_desc_by_store_id_and_purchases_qty[store_id]
        return batch_item

    def _take_next_batch(self) -> list[dict[str, str]]:
        batch: list[dict[str, str]] = []

        batch_size = 0
        all_batchs_taken = False

        while not all_batchs_taken and batch_size < self._batch_max_size:
            if not self._sorted_desc_by_store_id_and_purchases_qty:
                all_batchs_taken = True
                break

            batch_item = self._pop_next_batch_item()
            batch.append(batch_item)
            batch_size += 1

        return batch

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _send_batch_based_on_hash(self, batch: list[dict[str, str]]) -> None:
        user_batchs_by_hash: dict[int, list] = {}
        for batch_item in batch:
            if batch_item["user_id"] == "":
                logging.warning(
                    f"action: invalid_user_id | user_id: {batch_item['user_id']} | result: skipped"
                )
                continue
            user_id = int(float(batch_item["user_id"]))
            batch_item["user_id"] = str(user_id)
            mom_producers_amount = len(self._mom_producers)
            key = user_id % mom_producers_amount
            if key not in user_batchs_by_hash:
                user_batchs_by_hash[key] = []
            user_batchs_by_hash[key].append(batch_item)

        for key, user_batch in user_batchs_by_hash.items():
            mom_cleaned_data_producer = self._mom_producers[key]
            message = communication_protocol.encode_transactions_batch_message(
                user_batch
            )
            mom_cleaned_data_producer.send(message)
        logging.debug(
            f"action: message_sent | result: success | batch_size: {len(batch)}"
        )

    def _send_data_using_batchs(self) -> None:
        batch = self._take_next_batch()
        while len(batch) != 0 and self._is_running():
            self._send_batch_based_on_hash(batch)
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
            self._send_data_using_batchs()

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
