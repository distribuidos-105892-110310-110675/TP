import logging
import signal
from typing import Any, Callable, Optional

from middleware.rabbitmq_message_middleware_exchange import (
    RabbitMQMessageMiddlewareExchange,
)
from shared import communication_protocol


class FilterTransactionsByHour:

    # ============================== INITIALIZE ============================== #

    def __init_mom_consumer(
        self, host: str, exchange_prefix: str, routing_keys: list[str]
    ) -> None:
        self._mom_consumer = RabbitMQMessageMiddlewareExchange(
            host=host, exchange_name=exchange_prefix, route_keys=routing_keys
        )

    def __init_mom_producers(
        self,
        host: str,
        producer_exchange_prefix: str,
        producer_routing_key_prefix: str,
        producer_routing_keys_amount: int,
    ) -> None:
        self._current_producer_id = 0
        self._mom_producers = []
        exchange_name = producer_exchange_prefix
        for i in range(producer_routing_keys_amount):
            routing_key = [f"{producer_routing_key_prefix}.{i}"]
            self._mom_producers.append(
                RabbitMQMessageMiddlewareExchange(
                    host=host,
                    exchange_name=exchange_name,
                    route_keys=routing_key,
                )
            )

    def __init__(
        self,
        controller_id: int,
        rabbitmq_host: str,
        consumer_exchange_prefix: str,
        consumer_routing_key_prefix: str,
        producer_exchange_prefix: str,
        producer_routing_key_prefix: str,
        producer_routing_keys_amount: int,
        previous_controllers_amount: int,
        min_hour: int,
        max_hour: int,
    ) -> None:
        self._controller_id = controller_id

        self.__set_controller_as_not_running()
        signal.signal(signal.SIGTERM, self.__sigterm_signal_handler)

        self.__init_mom_consumer(
            rabbitmq_host,
            consumer_exchange_prefix,
            [f"{consumer_routing_key_prefix}.{self._controller_id}"],
        )
        self.__init_mom_producers(
            rabbitmq_host,
            producer_exchange_prefix,
            producer_routing_key_prefix,
            producer_routing_keys_amount,
        )

        self._eof_received_from_previous_controllers = 0
        self._previous_controllers_amount = previous_controllers_amount

        self.min_hour = min_hour
        self.max_hour = max_hour

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

    # ============================== PRIVATE - TRANSFORM DATA ============================== #

    def __transform_batch_item(self, batch_item: dict[str, str]) -> Optional[dict]:
        created_at = batch_item["created_at"]
        time = created_at.split(" ")[1]
        hour = int(time.split(":")[0])
        if self.min_hour <= hour and hour < self.max_hour:
            return batch_item
        return None

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
            if modified_item is not None:
                new_batch.append(modified_item)
        return str(encoder(message_type, new_batch))

    def __transform_batch_message(self, message: str) -> str:
        return self.__transform_batch_message_using(
            message,
            communication_protocol.decode_batch_message,
            communication_protocol.encode_batch_message,
        )

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def __mom_send_message_to_next(self, message: str) -> None:
        mom_producer = self._mom_producers[self._current_producer_id]
        mom_producer.send(message)

        self._current_producer_id += 1
        if self._current_producer_id >= len(self._mom_producers):
            self._current_producer_id = 0

    def __handle_data_batch_message(self, message: str) -> None:
        output_message = self.__transform_batch_message(message)
        if not communication_protocol.decode_is_empty_message(output_message):
            logging.debug(f"action: message_sent | result: success | message: {output_message}")
            self.__mom_send_message_to_next(output_message)

    def __handle_data_batch_eof(self, message: str) -> None:
        self._eof_received_from_previous_controllers += 1
        logging.debug(f"action: eof_received | result: success")

        if (
            self._eof_received_from_previous_controllers
            == self._previous_controllers_amount
        ):
            logging.info("action: all_eofs_received | result: success")
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
