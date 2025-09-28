import logging
import signal
import socket
from collections.abc import Callable
from typing import Any, Optional

from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol, constants


class Server:

    # ============================== INITIALIZE ============================== #

    def __init_mom_connections(self, host: str) -> None:
        self._mom_connections: dict[str, list[RabbitMQMessageMiddlewareQueue]] = {}

        for data_tag, cleaner_data in self._cleaners_data.items():

            workers_amount = cleaner_data[constants.WORKERS_AMOUNT]
            for id in range(workers_amount):
                queue_name = cleaner_data[constants.QUEUE_PREFIX_NAME] + f"-{id}"
                queue_producer = RabbitMQMessageMiddlewareQueue(host, queue_name)

                if self._mom_connections.get(data_tag) is None:
                    self._mom_connections[data_tag] = []
                self._mom_connections[data_tag].append(queue_producer)

    def __init_cleaners_data(self, cleaners_data: dict) -> None:
        self._cleaners_data = cleaners_data
        for data_tag in self._cleaners_data.keys():
            self._cleaners_data[data_tag]["current_worker_id"] = 0

    def __init__(
        self,
        port: int,
        listen_backlog: int,
        rabbitmq_host: str,
        cleaners_data: dict,
    ) -> None:
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)

        self.__set_server_as_not_running()
        signal.signal(signal.SIGTERM, self.__sigterm_signal_handler)

        self.__init_cleaners_data(cleaners_data)
        self.__init_mom_connections(rabbitmq_host)

        self._data_completed = {
            communication_protocol.MENU_ITEMS_BATCH_MSG_TYPE: False,
            communication_protocol.STORES_BATCH_MSG_TYPE: False,
            communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE: False,
            communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE: False,
            communication_protocol.USERS_BATCH_MSG_TYPE: False,
        }

    # ============================== PRIVATE - ACCESSING ============================== #

    def __is_running(self) -> bool:
        return self._server_running

    def __set_server_as_not_running(self) -> None:
        self._server_running = False

    def __set_server_as_running(self) -> None:
        self._server_running = True

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def __sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        logging.info("action: sigterm_signal_handler | result: in_progress")

        self.__set_server_as_not_running()

        self._server_socket.close()
        logging.debug("action: sigterm_server_socket_close | result: success")

        for mom_connections in self._mom_connections.values():
            for mom_connection in mom_connections:
                mom_connection.delete()
                mom_connection.close()
                logging.debug(
                    "action: message_middleware_connection_close | result: success"
                )

        logging.info("action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - ACCEPT CONNECTION ============================== #

    def __accept_new_connection(self) -> Optional[socket.socket]:
        client_connection: Optional[socket.socket] = None
        try:
            logging.info(
                "action: accept_connections | result: in_progress",
            )
            client_connection, addr = self._server_socket.accept()
            logging.info(
                f"action: accept_connections | result: success | ip: {addr[0]}",
            )
            return client_connection
        except OSError as e:
            if client_connection is not None:
                client_connection.shutdown(socket.SHUT_RDWR)
                client_connection.close()
                logging.debug("action: client_connection_close | result: success")
            logging.error(f"action: accept_connections | result: fail | error: {e}")
            return None

    # ============================== PRIVATE - SOCKET SEND/RECEIVE MESSAGES ============================== #

    def __socket_send_message(self, socket: socket.socket, message: str) -> None:
        logging.debug(f"action: send_message | result: in_progress | msg: {message}")

        socket.sendall(message.encode("utf-8"))

        logging.debug(f"action: send_message | result: success |  msg: {message}")

    def __socket_receive_message(self, socket: socket.socket) -> str:
        logging.debug(f"action: receive_message | result: in_progress")

        buffsize = constants.KiB * 100
        bytes_received = b""

        all_data_received = False
        while not all_data_received:
            chunk = socket.recv(buffsize)
            if len(chunk) == 0:
                logging.error(
                    f"action: receive_message | result: fail | error: unexpected disconnection",
                )
                raise OSError("Unexpected disconnection of the client")

            logging.debug(
                f"action: receive_chunk | result: success | chunk size: {len(chunk)}"
            )
            if chunk.endswith(communication_protocol.MSG_END_DELIMITER.encode("utf-8")):
                all_data_received = True

            bytes_received += chunk

        message = bytes_received.decode("utf-8")
        logging.debug(f"action: receive_message | result: success | msg: {message}")
        return message

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def __mom_send_message(self, data_tag: str, message: str) -> None:
        current_worker_id = self._cleaners_data[data_tag]["current_worker_id"]

        mom_connections = self._mom_connections[data_tag]
        mom_connection: RabbitMQMessageMiddlewareQueue = mom_connections[
            current_worker_id
        ]
        mom_connection.send(message)

        current_worker_id += 1

        workers_amount = self._cleaners_data[data_tag][constants.WORKERS_AMOUNT]
        if current_worker_id == workers_amount:
            current_worker_id = 0
        self._cleaners_data[data_tag]["current_worker_id"] = current_worker_id

    # ============================== PRIVATE - HANDLE CONNECTION ============================== #

    def __send_to_mom_based_on(
        self, message: str, message_type: str, data_tag: str
    ) -> None:
        if message == communication_protocol.encode_eof_message(message_type):
            self._data_completed[message_type] = True
            logging.info(
                f"action: {data_tag}_batch_received | result: EOF_reached",
            )
        else:
            self.__mom_send_message(data_tag, message)

    def __handle_menu_items_batch_message(self, message: str) -> None:
        self.__send_to_mom_based_on(
            message,
            communication_protocol.MENU_ITEMS_BATCH_MSG_TYPE,
            constants.MENU_ITEMS,
        )

    def __handle_stores_batch_message(self, message: str) -> None:
        self.__send_to_mom_based_on(
            message,
            communication_protocol.STORES_BATCH_MSG_TYPE,
            constants.STORES,
        )

    def __handle_transaction_items_batch_message(self, message: str) -> None:
        self.__send_to_mom_based_on(
            message,
            communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE,
            constants.TRANSACTION_ITEMS,
        )

    def __handle_transactions_batch_message(self, message: str) -> None:
        self.__send_to_mom_based_on(
            message,
            communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE,
            constants.TRANSACTIONS,
        )

    def __handle_users_batch_message(self, message: str) -> None:
        self.__send_to_mom_based_on(
            message,
            communication_protocol.USERS_BATCH_MSG_TYPE,
            constants.USERS,
        )

    def __handle_client_message(self, message: str) -> None:
        match communication_protocol.decode_message_type(message):
            case communication_protocol.MENU_ITEMS_BATCH_MSG_TYPE:
                self.__handle_menu_items_batch_message(message)
            case communication_protocol.STORES_BATCH_MSG_TYPE:
                self.__handle_stores_batch_message(message)
            case communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE:
                self.__handle_transaction_items_batch_message(message)
            case communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE:
                self.__handle_transactions_batch_message(message)
            case communication_protocol.USERS_BATCH_MSG_TYPE:
                self.__handle_users_batch_message(message)
            case _:
                raise ValueError(
                    f'Invalid message type received from client "{communication_protocol.decode_message_type(message)}"'
                )

    def __with_each_client_message_do(
        self, received_message: str, callback: Callable
    ) -> None:
        messages = received_message.split(communication_protocol.MSG_END_DELIMITER)
        for message in messages:
            if not self.__is_running():
                break
            if message == "":
                continue
            message += communication_protocol.MSG_END_DELIMITER
            callback(message)

    def __handle_client_connection(self, client_socket: socket.socket) -> None:
        while self.__is_running() and not all(self._data_completed.values()):
            if not self.__is_running():
                return

            received_message = self.__socket_receive_message(client_socket)
            self.__with_each_client_message_do(
                received_message,
                self.__handle_client_message,
            )

        logging.info("action: all_data_received | result: success")

        # keep listening for results from queue
        # start sending messages

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        logging.info("action: server_startup | result: success")

        self.__set_server_as_running()
        try:
            while self.__is_running():
                client_socket = self.__accept_new_connection()
                if client_socket is None:
                    continue

                try:
                    self.__handle_client_connection(client_socket)
                except Exception as e:
                    client_socket.close()
                    logging.debug("action: client_connection_close | result: success")
                    raise e
        except Exception as e:
            logging.error(f"action: server_run | result: fail | error: {e}")
            raise e
        finally:
            self._server_socket.close()
            logging.debug("action: server_socket_close | result: success")

        logging.info("action: server_shutdown | result: success")
