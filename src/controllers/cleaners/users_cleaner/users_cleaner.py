import logging
import signal
from typing import Any, Callable

from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol


class UsersCleaner:

    # ============================== INITIALIZE ============================== #

    def __init_mom_cleaner_connection(
        self, host: str, cleaner_queue_prefix: str
    ) -> None:
        queue_name = f"{cleaner_queue_prefix}-{self._cleaner_id}"
        self._mom_cleaner_consumer = RabbitMQMessageMiddlewareQueue(
            host=host, queue_name=queue_name
        )

    def __init_mom_join_connection(self, host: str, join_queue_prefix: str) -> None:
        queue_name = join_queue_prefix
        self._mom_join_producer = RabbitMQMessageMiddlewareQueue(
            host=host, queue_name=queue_name
        )

    def __init__(
        self,
        cleaner_id: int,
        rabbitmq_host: str,
        data_queue_prefix: str,
        cleaned_data_queue_prefix: str,
    ) -> None:
        self._cleaner_id = cleaner_id
        self.__set_controller_as_not_running()
        signal.signal(signal.SIGTERM, self.__sigterm_signal_handler)

        self.__init_mom_cleaner_connection(
            rabbitmq_host,
            data_queue_prefix,
        )
        self.__init_mom_join_connection(
            rabbitmq_host,
            cleaned_data_queue_prefix,
        )

    # ============================== PRIVATE - ACCESSING ============================== #

    def __is_running(self) -> bool:
        return self._server_running

    def __set_controller_as_not_running(self) -> None:
        self._server_running = False

    def __set_controller_as_running(self) -> None:
        self._server_running = True

    def __columns_to_keep(self) -> list[str]:
        return [
            "user_id",
            "birthdate",
        ]

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
    def __filter_items_using(
        self, message: str, decoder: Callable, encoder: Callable
    ) -> str:
        filtered_items_batch = []

        for item in decoder(message):
            filtered_item = self.__filter_item(item)
            filtered_items_batch.append(filtered_item)

        return str(encoder(filtered_items_batch))

    # @TODO: this is the only should send
    def __filter_message(self, message: str) -> str:
        return self.__filter_items_using(
            message,
            communication_protocol.decode_users_batch_message,
            communication_protocol.encode_users_batch_message,
        )

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def __mom_send_message_to_next(self, message: str) -> None:
        self._mom_join_producer.send(message)

    def __handle_data_batch_message(self, message: str) -> None:
        filtered_message = self.__filter_message(message)
        logging.debug(f"action: message_filtered | result: success")
        self.__mom_send_message_to_next(filtered_message)

    def __handle_data_batch_eof(self, message: str) -> None:
        self._mom_join_producer.send(message)
        logging.debug(f"action: eof_sent_to_join | result: success")

    def __handle_received_data(self, message_as_bytes: bytes) -> None:
        if not self.__is_running():
            self._mom_cleaner_consumer.stop_consuming()
            return

        message = message_as_bytes.decode("utf-8")
        message_type = communication_protocol.decode_message_type(message)
        match message_type:
            case communication_protocol.USERS_BATCH_MSG_TYPE:
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
        self._mom_join_producer.delete()
        self._mom_join_producer.close()
        logging.debug("action: mom_join_connection_close | result: success")

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
