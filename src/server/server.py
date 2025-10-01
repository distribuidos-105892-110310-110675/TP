import logging
import signal
import socket
from collections.abc import Callable
from typing import Any, Optional

from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol, constants


class Server:

    # ============================== INITIALIZE ============================== #

    def __init_cleaners_data(self, cleaners_data: dict) -> None:
        self._cleaners_data = cleaners_data
        for data_type in self._cleaners_data.keys():
            self._cleaners_data[data_type]["current_worker_id"] = 0

    def __init_output_builders_data(self, output_builders_data: dict) -> None:
        self._output_builders_data = output_builders_data

    def __init_client_data_batch_stats(self) -> None:
        self._client_data_batch_completed = {
            communication_protocol.MENU_ITEMS_BATCH_MSG_TYPE: False,
            communication_protocol.STORES_BATCH_MSG_TYPE: False,
            communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE: False,
            communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE: False,
            communication_protocol.USERS_BATCH_MSG_TYPE: False,
        }

    def __init_output_builders_data_stats(self) -> None:
        self._output_builders_eof_received = {
            constants.QUERY_RESULT_1X: 0,
            constants.QUERY_RESULT_21: 0,
            constants.QUERY_RESULT_22: 0,
            constants.QUERY_RESULT_3X: 0,
            constants.QUERY_RESULT_4X: 0,
        }

    def __init_mom_cleaners_connections(self, host: str) -> None:
        self._mom_cleaners_connections: dict[
            str, list[RabbitMQMessageMiddlewareQueue]
        ] = {}

        for data_type, cleaner_data in self._cleaners_data.items():
            workers_amount = cleaner_data[constants.WORKERS_AMOUNT]
            for id in range(workers_amount):
                queue_name = cleaner_data[constants.QUEUE_PREFIX] + f"-{id}"
                queue_producer = RabbitMQMessageMiddlewareQueue(host, queue_name)

                if self._mom_cleaners_connections.get(data_type) is None:
                    self._mom_cleaners_connections[data_type] = []
                self._mom_cleaners_connections[data_type].append(queue_producer)

    def __init_mom_output_builders_connection(self, host: str) -> None:
        (_, output_builder_data) = next(iter(self._output_builders_data.items()))
        queue_name = output_builder_data[constants.QUEUE_PREFIX]
        queue_consumer = RabbitMQMessageMiddlewareQueue(host, queue_name)
        self._mom_output_builders_connection = queue_consumer

    def __init__(
        self,
        port: int,
        listen_backlog: int,
        rabbitmq_host: str,
        cleaners_data: dict,
        output_builders_data: dict,
    ) -> None:
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)

        self.__set_server_as_not_running()
        signal.signal(signal.SIGTERM, self.__sigterm_signal_handler)

        self.__init_cleaners_data(cleaners_data)
        self.__init_output_builders_data(output_builders_data)

        self.__init_client_data_batch_stats()
        self.__init_output_builders_data_stats()

        self.__init_mom_cleaners_connections(rabbitmq_host)
        self.__init_mom_output_builders_connection(rabbitmq_host)

        self._temp_buffer = b""

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

        self._mom_output_builders_connection.stop_consuming()
        logging.debug("action: sigterm_mom_stop_consuming | result: success")

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

        buffsize = constants.KiB
        bytes_received = self._temp_buffer
        self._temp_buffer = b""

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

            if communication_protocol.MSG_END_DELIMITER.encode("utf-8") in chunk:
                index = chunk.rindex(communication_protocol.MSG_END_DELIMITER.encode("utf-8"))
                bytes_received += chunk[: index + len(communication_protocol.MSG_END_DELIMITER)]
                self._temp_buffer = chunk[index + len(communication_protocol.MSG_END_DELIMITER) :]
                all_data_received = True
            else:
                bytes_received += chunk

        message = bytes_received.decode("utf-8")
        logging.debug(f"action: receive_message | result: success | msg: {message}")
        return message

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def __mom_send_message_to_next(self, data_type: str, message: str) -> None:
        current_worker_id = self._cleaners_data[data_type]["current_worker_id"]

        mom_connections = self._mom_cleaners_connections[data_type]
        mom_connection: RabbitMQMessageMiddlewareQueue = mom_connections[
            current_worker_id
        ]
        mom_connection.send(message)

        current_worker_id += 1

        workers_amount = self._cleaners_data[data_type][constants.WORKERS_AMOUNT]
        if current_worker_id == workers_amount:
            current_worker_id = 0
        self._cleaners_data[data_type]["current_worker_id"] = current_worker_id

    # ============================== PRIVATE - RECEIVE CLIENT DATA ============================== #

    def __handle_data_batch_message(self, message: str) -> None:
        data_type = communication_protocol.decode_message_type(message)
        self.__mom_send_message_to_next(data_type, message)

    def __handle_data_batch_eof_message(self, message: str) -> None:
        data_type = communication_protocol.decode_eof_message(message)
        if data_type not in self._client_data_batch_completed:
            raise ValueError(
                f'Invalid EOF message type received from client "{data_type}"'
            )
        self._client_data_batch_completed[data_type] = True
        logging.info(f"action: {data_type}_batch_received | result: EOF_reached")

        for mom_connection in self._mom_cleaners_connections[data_type]:
            mom_connection.send(message)

    def __handle_client_message(self, message: str) -> None:
        message_type = communication_protocol.decode_message_type(message)
        match message_type:
            case (
                communication_protocol.MENU_ITEMS_BATCH_MSG_TYPE
                | communication_protocol.STORES_BATCH_MSG_TYPE
                | communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE
                | communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE
                | communication_protocol.USERS_BATCH_MSG_TYPE
            ):
                self.__handle_data_batch_message(message)
            case communication_protocol.EOF:
                self.__handle_data_batch_eof_message(message)
            case _:
                raise ValueError(
                    f'Invalid message type received from client "{message_type}"'
                )

    def __with_each_message_do(
        self,
        received_message: str,
        callback: Callable,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        messages = received_message.split(communication_protocol.MSG_END_DELIMITER)
        for message in messages:
            if not self.__is_running():
                break
            if message == "":
                continue
            message += communication_protocol.MSG_END_DELIMITER
            callback(message, *args, **kwargs)

    def __receive_all_data_from_client(self, client_socket: socket.socket) -> None:
        while not all(self._client_data_batch_completed.values()):
            if not self.__is_running():
                return

            received_message = self.__socket_receive_message(client_socket)
            self.__with_each_message_do(
                received_message,
                self.__handle_client_message,
            )

        logging.info("action: all_data_received | result: success")

    # ============================== PRIVATE - RECEIVE RESULTS ============================== #

    def __all_eof_received_from_output_builders(self) -> bool:
        for data_type, eof_received in self._output_builders_eof_received.items():
            workers_amount = self._output_builders_data[data_type][
                constants.WORKERS_AMOUNT
            ]
            if eof_received < workers_amount:
                return False
        return True

    def __handle_query_result_batch_message(
        self, client_socket: socket.socket, message: str
    ) -> None:
        self.__socket_send_message(client_socket, message)

    def __handle_query_result_eof_message(
        self, client_socket: socket.socket, message: str
    ) -> None:
        data_type = communication_protocol.decode_eof_message(message)
        if data_type not in self._output_builders_eof_received:
            raise ValueError(
                f'Invalid EOF message type received from output builder "{data_type}"'
            )
        self._output_builders_eof_received[data_type] += 1
        logging.info(f"action: eof_{data_type}_result_received | result: success")

        workers_amount = self._output_builders_data[data_type][constants.WORKERS_AMOUNT]
        if self._output_builders_eof_received[data_type] == workers_amount:
            self.__socket_send_message(client_socket, message)
            logging.info(
                f"action: eof_{data_type}_results_to_client_sent | result: success"
            )

    def __handle_output_builder_message(self, client_socket: socket.socket) -> Callable:
        def __on_message_callback(message_as_bytes: bytes) -> None:
            message = message_as_bytes.decode("utf-8")
            message_type = communication_protocol.decode_message_type(message)
            match message_type:
                case (
                    communication_protocol.QUERY_RESULT_1X_MSG_TYPE
                    | communication_protocol.QUERY_RESULT_21_MSG_TYPE
                    | communication_protocol.QUERY_RESULT_22_MSG_TYPE
                    | communication_protocol.QUERY_RESULT_3X_MSG_TYPE
                    | communication_protocol.QUERY_RESULT_4X_MSG_TYPE
                ):
                    self.__handle_query_result_batch_message(client_socket, message)
                case communication_protocol.EOF:
                    self.__handle_query_result_eof_message(client_socket, message)
                case _:
                    raise ValueError(
                        f'Invalid message type received from client "{message_type}"'
                    )

            if self.__all_eof_received_from_output_builders():
                self._mom_output_builders_connection.stop_consuming()
                logging.info("action: all_results_received | result: success")

        return __on_message_callback

    def __receive_all_query_results_from_output_builders(
        self, client_socket: socket.socket
    ) -> None:
        self._mom_output_builders_connection.start_consuming(
            self.__handle_output_builder_message(client_socket)
        )

    # ============================== PRIVATE - HANDLE CLIENT CONNECTION ============================== #

    def __handle_client_connection(self, client_socket: socket.socket) -> None:
        # @TODO: handle handshake to store client data
        # we can assing an uuid to the client
        # now we are initializating all client data on init
        self.__receive_all_data_from_client(client_socket)
        self.__receive_all_query_results_from_output_builders(client_socket)

    # ============================== PRIVATE - RUN ============================== #

    def __run(self) -> None:
        self.__set_server_as_running()

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

    def __close_all_mom_connections(self) -> None:
        for mom_cleaner_connections in self._mom_cleaners_connections.values():
            for mom_cleaner_connection in mom_cleaner_connections:
                mom_cleaner_connection.delete()
                mom_cleaner_connection.close()
                logging.debug("action: mom_cleaner_connection_close | result: success")

        self._mom_output_builders_connection.delete()
        self._mom_output_builders_connection.close()
        logging.debug("action: mom_output_builder_connection_close | result: success")

    def __ensure_connections_close_after_doing(self, callback: Callable) -> None:
        try:
            callback()
        except Exception as e:
            logging.error(f"action: server_run | result: fail | error: {e}")
            raise e
        finally:
            self._server_socket.close()
            logging.debug("action: server_socket_close | result: success")

            self.__close_all_mom_connections()
            logging.debug("action: all_mom_connections_close | result: success")

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        logging.info("action: server_startup | result: success")

        self.__ensure_connections_close_after_doing(self.__run)

        logging.info("action: server_shutdown | result: success")
