import logging
import signal
from typing import Any, Callable

from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol


class TransactionItemsCleaner:

    # ============================== INITIALIZE ============================== #

    def __init_mom_cleaner_connection(
        self, host: str, cleaner_queue_prefix: str
    ) -> None:
        queue_name = f"{cleaner_queue_prefix}-{self._cleaner_id}"
        self._mom_cleaner_consumer = RabbitMQMessageMiddlewareQueue(
            host=host, queue_name=queue_name
        )

    def __init_mom_filters_connections(
        self, host: str, filters_queue_prefix: str, filters_amount: int
    ) -> None:
        self._current_filter_id = 0
        self._mom_filters_producers: list[RabbitMQMessageMiddlewareQueue] = []
        for id in range(filters_amount):
            queue_name = f"{filters_queue_prefix}-{id}"
            mom_filter_producer = RabbitMQMessageMiddlewareQueue(
                host=host, queue_name=queue_name
            )
            self._mom_filters_producers.append(mom_filter_producer)

    def __init__(
        self,
        cleaner_id: int,
        rabbitmq_host: str,
        cleaner_queue_prefix: str,
        filters_queue_prefix: str,
        filters_amount: int,
    ) -> None:
        self._cleaner_id = cleaner_id

        self.__set_controller_as_not_running()
        signal.signal(signal.SIGTERM, self.__sigterm_signal_handler)

        self.__init_mom_cleaner_connection(
            rabbitmq_host,
            cleaner_queue_prefix,
        )
        self.__init_mom_filters_connections(
            rabbitmq_host,
            filters_queue_prefix,
            filters_amount,
        )

    # ============================== PRIVATE - ACCESSING ============================== #

    def __is_running(self) -> bool:
        return self._server_running

    def __set_controller_as_not_running(self) -> None:
        self._server_running = False

    def __set_controller_as_running(self) -> None:
        self._server_running = True

    def __columns_to_keep(self) -> list[str]:
        return ["created_at", "item_id", "subtotal"]

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def __sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        logging.info("action: sigterm_signal_handler | result: in_progress")

        self.__set_controller_as_not_running()

        self._mom_cleaner_consumer.stop_consuming()
        logging.debug("action: sigterm_mom_stop_consuming | result: success")

        logging.info("action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - FILTER ============================== #

    # @TODO: this is something that can be abstracted to a base cleaner class
    def __filter_item(self, item: dict) -> dict:
        filtered_item = {}
        for column in self.__columns_to_keep():
            filtered_item[column] = item[column]
        return filtered_item

    # @TODO: this is something that can be abstracted to a base cleaner class
    def __filter_message(self, message: str) -> str:
        filtered_transaction_items_batch = []

        for item in communication_protocol.decode_transaction_items_batch_message(
            message
        ):
            filtered_item = self.__filter_item(item)
            filtered_transaction_items_batch.append(filtered_item)

        return communication_protocol.encode_transaction_items_batch_message(
            filtered_transaction_items_batch
        )

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def __mom_send_message_to_next(self, message: str) -> None:
        mom_filter_producer = self._mom_filters_producers[self._current_filter_id]
        mom_filter_producer.send(message)

        self._current_filter_id += 1
        if self._current_filter_id >= len(self._mom_filters_producers):
            self._current_filter_id = 0

    def __handle_data_batch_message(self, message: str) -> None:
        filtered_message = self.__filter_message(message)
        self.__mom_send_message_to_next(filtered_message)

    def __handle_data_batch_eof(self, message: str) -> None:
        for mom_filter_producer in self._mom_filters_producers:
            mom_filter_producer.send(message)

    def __handle_received_data(self, message_as_bytes: bytes) -> None:
        if not self.__is_running():
            self._mom_cleaner_consumer.stop_consuming()
            return

        message = message_as_bytes.decode("utf-8")
        message_type = communication_protocol.decode_message_type(message)
        match message_type:
            case communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE:
                self.__handle_data_batch_message(message)
            case communication_protocol.EOF:
                self.__handle_data_batch_eof(message)
            case _:
                raise ValueError(f"Invalid message type received: {message_type}")

        # @TODO: this is something that can be abstracted to a base cleaner class

    # ============================== PRIVATE - RUN ============================== #

    def __run(self) -> None:
        self.__set_controller_as_running()
        self._mom_cleaner_consumer.start_consuming(self.__handle_received_data)

    # @TODO: this is something that can be abstracted to a base cleaner class
    def __close_all_mom_connections(self) -> None:
        for mom_filter_producer in self._mom_filters_producers:
            mom_filter_producer.delete()
            mom_filter_producer.close()
            logging.debug("action: mom_filter_connection_close | result: success")

        self._mom_cleaner_consumer.delete()
        self._mom_cleaner_consumer.close()
        logging.debug("action: mom_cleaner_connection_close | result: success")

    def __ensure_connections_close_after_doing(self, callback: Callable) -> None:
        try:
            callback()
        except Exception as e:
            logging.error(f"action: cleaner_run | result: fail | error: {e}")
            raise e
        finally:
            self.__close_all_mom_connections()
            logging.debug("action: all_mom_connections_close | result: success")

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        logging.info("action: cleaner_startup | result: success")

        self.__ensure_connections_close_after_doing(self.__run)

        logging.info("action: cleaner_shutdown | result: success")
