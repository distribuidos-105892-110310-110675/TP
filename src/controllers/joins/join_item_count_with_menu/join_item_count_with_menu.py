import signal
import logging
from typing import Any, Callable
from shared import communication_protocol
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from middleware.rabbitmq_message_middleware_exchange import RabbitMQMessageMiddlewareExchange

class JoinItemCountWithMenu:
    
    def __init_mom_consumers(self, host: str, consumer_queue_prefix: str, consumer_exchange_prefix: str, 
                                routing_keys: list[str]) -> None:
        queue_name = f"{consumer_queue_prefix}-{self._controller_id}"
        self._mom_queue_consumer = RabbitMQMessageMiddlewareQueue(
            host=host, queue_name=queue_name
        )
        self._mom_exchange_consumer = RabbitMQMessageMiddlewareExchange(
            host=host, exchange_name=consumer_exchange_prefix, route_keys=routing_keys
        )


    def __init_mom_producers(
        self, host: str, producer_queue_prefix: str, producer_queue_amount: int
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
        previous_menu_items_senders: int,
        previous_controller_amount: int,
        consumer_queue_prefix: str,
        producer_queue_prefix: str,
        producer_queue_amount: int,
        consumer_exchange_prefix: str,
        consumer_routing_key_prefix: str, 
    ) -> None:
        self._controller_id = controller_id

        self.__set_controller_as_not_running()
        signal.signal(signal.SIGTERM, self.__sigterm_signal_handler)

        self.__init_mom_consumers(
            rabbitmq_host,
            consumer_queue_prefix,
            consumer_exchange_prefix, 
            [f"{consumer_routing_key_prefix}-{self._controller_id}"]
        )
        self.__init_mom_producers(
            rabbitmq_host,
            producer_queue_prefix,
            producer_queue_amount,
        )

        self._previous_menu_items_senders = previous_menu_items_senders
        self._previous_controllers_amount = previous_controller_amount
        self._eof_received_from_previous_controllers = 0
        self._eof_received_from_menu_item_cleaners = 0
        self._menu_items = []
        self._received_all_menu_items = False

    # ============================== PRIVATE - ACCESSING ============================== #

    def __is_running(self) -> bool:
        return self._server_running

    def __set_controller_as_not_running(self) -> None:
        self._server_running = False

    def __set_controller_as_running(self) -> None:
        self._server_running = True

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def __sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        logging.info("action: sigterm_signal_handler | result: in_progress")

        self.__set_controller_as_not_running()

        self._mom_exchange_consumer.stop_consuming()
        self._mom_queue_consumer.stop_consuming()
        logging.debug("action: sigterm_mom_stop_consuming | result: success")

        logging.info("action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - JOIN ============================== #

    def __join_with_menu_items_using(self, message: str, decoder: Callable, encoder: Callable) -> str:
        joined = []
        for item in decoder(message):
            for m_item in self._menu_items:
                if item.get('item_id') == m_item.get('item_id'):
                    joined.append({**item, **m_item})
        return str(encoder(joined))

    def __join_with_menu_items(self, message: str) -> str:
        return self.__join_with_menu_items_using(message, communication_protocol.decode_transaction_items_batch_message, communication_protocol.encode_transaction_items_batch_message)

    def __receive_menu_items_using(self, message: str, decoder: Callable):
        for item in decoder(message):
            self._menu_items.append(item)

    def __receive_menu_items(self, message: str):
        self.__receive_menu_items_using(message, communication_protocol.decode_menu_items_batch_message)

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def __mom_send_message_to_next(self, message: str) -> None:
        mom_cleaned_data_producer = self._mom_producers[
            self._current_producer_id
        ]
        mom_cleaned_data_producer.send(message)

        self._current_producer_id += 1
        if self._current_producer_id >= len(
            self._mom_producers
        ):
            self._current_producer_id = 0

    def __handle_data_batch_message(self, message: str) -> None:
        joined_items = self.__join_with_menu_items(message)
        self.__mom_send_message_to_next(joined_items)

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

    def __handle_menu_items_eof(self, message: str) -> None:
        self._eof_received_from_menu_item_cleaners += 1
        logging.debug(f"action: eof_received | result: success")
        if (self._eof_received_from_menu_item_cleaners == self._previous_menu_items_senders):
            logging.info(f"action: all_eofs_received | result: success")
            self._received_all_menu_items = True

    def __handle_menu_items(self, message: str) -> None:
        if not self._received_all_menu_items:
            self.__receive_menu_items(message)
        
    def __handle_received_data(self, message_as_bytes: bytes) -> None:
        if not self.__is_running():
            self._mom_queue_consumer.stop_consuming()
            return

        message = message_as_bytes.decode("utf-8")
        message_type = communication_protocol.decode_message_type(message)
        match message_type:
            case communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE:
                self.__handle_data_batch_message(message)
            case communication_protocol.MENU_ITEMS_BATCH_MSG_TYPE:
                self.__handle_menu_items(message)
            case communication_protocol.EOF:
                message_body = communication_protocol.get_message_payload(message)
                match message_body:
                    case communication_protocol.MENU_ITEMS_BATCH_MSG_TYPE:
                        self.__handle_menu_items_eof(message)
                    case communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE:
                        self.__handle_data_batch_eof(message)
            case _:
                raise ValueError(f"Invalid message type received: {message_type}")

    # ============================== PRIVATE - RUN ============================== #

    def __run(self) -> None:
        self.__set_controller_as_running()
        self._mom_exchange_consumer.start_consuming(self.__handle_received_data)
        self._mom_queue_consumer.start_consuming(self.__handle_received_data)

    def __close_all_mom_connections(self) -> None:
        for mom_producer in self._mom_producers:
            mom_producer.delete()
            mom_producer.close()
            logging.debug("action: mom_cleaned_data_producer_close | result: success")

        self._mom_exchange_consumer.delete()
        self._mom_exchange_consumer.close()
        self._mom_queue_consumer.delete()
        self._mom_queue_consumer.close()
        logging.debug("action: mom_data_consumer_close | result: success")

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