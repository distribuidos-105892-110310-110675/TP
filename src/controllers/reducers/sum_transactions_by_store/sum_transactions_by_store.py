import logging
import signal
from typing import Any, Callable
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol


class SumTransactionsByStore:

    # ============================== INITIALIZE ============================== #

    def __init_mom_consumers(self, host: str, consumer_queue_prefix: str)-> None:
        queue_name = f"{consumer_queue_prefix}-{self._controller_id}"
        self._mom_consumer = RabbitMQMessageMiddlewareQueue(
            host=host, queue_name=queue_name
        )

    def __init_mom_producers(
        self,
        host: str,
        producer_queue_prefix: str,
        producer_queues_amount: int,
    ) -> None:
        self._current_producer_id = 0
        self._mom_producers : list[RabbitMQMessageMiddlewareQueue]= []
        for i in range(producer_queues_amount):
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
        producer_queue_prefix: str,
        consumer_queue_prefix: str,
        previous_controllers_amount: int,
        next_controllers_amount: int,
        batch_max_size: int,
    ) -> None:
        self._controller_id = controller_id

        self.__set_controller_as_not_running()
        signal.signal(signal.SIGTERM, self.__sigterm_signal_handler)

        self.__init_mom_consumers(
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

        self._purchase_counts: dict[tuple[str, str], float] = {}

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

    # ============================== PRIVATE - HANDLE DATA ============================== #



    def __add_purchase(self, store_id: str, year_half_created_at: str, final_amount: float) -> None:
        key = (store_id, year_half_created_at)
        if key not in self._purchase_counts:
            self._purchase_counts[key] = 0
        self._purchase_counts[key] += final_amount

    def __pop_next_batch_item(self) -> dict[str, str]:
        (item_id, year_half_created_at), tpv = self._purchase_counts.popitem()
        item = {}
        item["store_id"] = item_id
        item["year_half_created_at"] = year_half_created_at
        item["tpv"] = str(tpv)
        return item

    def __take_next_batch(self) -> list[dict[str, str]]:
        batch: list[dict[str, str]] = []

        batch_size = 0
        all_batchs_taken = False

        while not all_batchs_taken and batch_size < self._batch_max_size:
            if not self._purchase_counts:
                all_batchs_taken = True
                break

            item = self.__pop_next_batch_item()
            batch.append(item)
            batch_size += 1

        return batch

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def __send_data_using_batchs(self,) -> None:
        while self.__is_running():
            batch = self.__take_next_batch()
            if len(batch) == 0:
                break
            message = communication_protocol.encode_transaction_items_batch_message(batch)
            mom_producer = self._mom_producers[self._current_producer_id]
            mom_producer.send(message)
            if self._current_producer_id >= len(self._mom_producers) - 1:
                self._current_producer_id = 0
            else:
                self._current_producer_id += 1
            logging.debug(
                f"action: message_sent | result: success | message: {message}"
            )

    def __handle_data_batch_message(self, message: str) -> None:
        batch = communication_protocol.decode_batch_message(message)
        for batch_item in batch:
            store_id = batch_item["store_id"]
            year_half_created_at = batch_item["year_half_created_at"]
            final_amount = float(batch_item["final_amount"])
            self.__add_purchase(store_id, year_half_created_at, final_amount)

    def __handle_data_batch_eof(self, message: str) -> None:
        self._eof_received_from_previous_controllers += 1
        logging.debug(f"action: eof_received | result: success")

        if (
            self._eof_received_from_previous_controllers
            == self._previous_controllers_amount
        ):
            logging.info("action: all_eofs_received | result: success")

            self.__send_data_using_batchs()

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