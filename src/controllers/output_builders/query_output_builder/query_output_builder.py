import logging
import signal
from abc import ABC, abstractmethod
from typing import Any, Callable, Optional

from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol


class QueryOutputBuilder(ABC):

    # ============================== INITIALIZE ============================== #

    def __init_mom_consumer(self, host: str, data_queue_prefix: str) -> None:
        queue_name = f"{data_queue_prefix}-{self._controller_id}"
        self._mom_consumer = RabbitMQMessageMiddlewareQueue(
            host=host, queue_name=queue_name
        )

    def __init_mom_producer(self, host: str, producer_queue_prefix: str) -> None:
        queue_name = producer_queue_prefix
        self._mom_producer = RabbitMQMessageMiddlewareQueue(
            host=host, queue_name=queue_name
        )

    def __init__(
        self,
        controller_id: int,
        rabbitmq_host: str,
        consumer_queue_prefix: str,
        producer_queue_prefix: str,
        previous_controllers_amount: int,
    ) -> None:
        self._controller_id = controller_id

        self.__set_controller_as_not_running()
        signal.signal(signal.SIGTERM, self.__sigterm_signal_handler)

        self.__init_mom_consumer(rabbitmq_host, consumer_queue_prefix)
        self.__init_mom_producer(rabbitmq_host, producer_queue_prefix)

        self._eof_received_from_previous_controllers = 0
        self._previous_controllers_amount = previous_controllers_amount

    # ============================== PRIVATE - ACCESSING ============================== #

    def __is_running(self) -> bool:
        return self._server_running

    def __set_controller_as_not_running(self) -> None:
        self._server_running = False

    def __set_controller_as_running(self) -> None:
        self._server_running = True

    # ============================== PRIVATE - INTERFACE ============================== #

    @abstractmethod
    def columns_to_keep(self) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def output_message_type(self) -> str:
        raise NotImplementedError

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def __sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        logging.info("action: sigterm_signal_handler | result: in_progress")

        self.__set_controller_as_not_running()

        self._mom_consumer.stop_consuming()
        logging.debug("action: sigterm_mom_stop_consuming | result: success")

        logging.info("action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - OUTPUT BUILDER ============================== #

    def __transform_batch_item(self, batch_item: dict[str, str]) -> dict:
        modified_item_batch = {}
        for column in self.columns_to_keep():
            modified_item_batch[column] = batch_item[column]
        return modified_item_batch

    def __transform_batch_message_using(
        self,
        message: str,
        decoder: Callable,
        encoder: Callable,
        output_message_type: Optional[str] = None,
    ) -> str:
        message_type = output_message_type
        if output_message_type is None:
            message_type = communication_protocol.decode_message_type(message)

        new_batch = []
        for item in decoder(message):
            modified_item = self.__transform_batch_item(item)
            new_batch.append(modified_item)
        return str(encoder(message_type, new_batch))

    def __transform_batch_message(self, message: str) -> str:
        return self.__transform_batch_message_using(
            message,
            communication_protocol.decode_batch_message,
            communication_protocol.encode_batch_message,
            self.output_message_type(),
        )

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def __handle_data_batch_message(self, message: str) -> None:
        output_message = self.__transform_batch_message(message)
        self._mom_producer.send(output_message)

    def __handle_data_batch_eof(self, message: str) -> None:
        self._eof_received_from_previous_controllers += 1
        logging.debug(f"action: eof_received | result: success")

        if (
            self._eof_received_from_previous_controllers
            == self._previous_controllers_amount
        ):
            logging.info("action: all_eofs_received | result: success")
            message = communication_protocol.encode_eof_message(
                self.output_message_type()
            )
            self._mom_producer.send(message)
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
        self._mom_producer.delete()
        self._mom_producer.close()
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
