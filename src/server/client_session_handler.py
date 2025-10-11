import logging
import multiprocessing
import signal
import socket
import uuid
from typing import Any, Callable

from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol, constants


class ClientSessionHandler:

    # ============================== INITIALIZE ============================== #

    def _init_cleaners_data(self, cleaners_data: dict) -> None:
        self._cleaners_data = cleaners_data
        for data_type in self._cleaners_data.keys():
            self._cleaners_data[data_type]["current_worker_id"] = 0

    def _init_output_builders_data(self, output_builders_data: dict) -> None:
        self._output_builders_data = output_builders_data

    def _init_client_data_batch_stats(self) -> None:
        self._client_data_batch_completed = {
            communication_protocol.MENU_ITEMS_BATCH_MSG_TYPE: False,
            communication_protocol.STORES_BATCH_MSG_TYPE: False,
            communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE: False,
            communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE: False,
            communication_protocol.USERS_BATCH_MSG_TYPE: False,
        }

    def _init_output_builders_data_stats(self) -> None:
        self._output_builders_eof_received = {
            constants.QUERY_RESULT_1X: 0,
            constants.QUERY_RESULT_21: 0,
            constants.QUERY_RESULT_22: 0,
            constants.QUERY_RESULT_3X: 0,
            constants.QUERY_RESULT_4X: 0,
        }

    def _init_mom_producers(self, rabbitmq_host: str) -> None:
        self._mom_cleaners_connections: dict[
            str, list[RabbitMQMessageMiddlewareQueue]
        ] = {}

        for data_type, cleaner_data in self._cleaners_data.items():
            workers_amount = cleaner_data[constants.WORKERS_AMOUNT]
            for id in range(workers_amount):
                queue_name = f"{cleaner_data[constants.QUEUE_PREFIX]}-{id}"
                queue_producer = RabbitMQMessageMiddlewareQueue(
                    rabbitmq_host, queue_name
                )

                if self._mom_cleaners_connections.get(data_type) is None:
                    self._mom_cleaners_connections[data_type] = []
                self._mom_cleaners_connections[data_type].append(queue_producer)

    def _init_mom_consumers(self, rabbitmq_host: str) -> None:
        (_, output_builder_data) = next(iter(self._output_builders_data.items()))
        queue_name = f"{output_builder_data[constants.QUEUE_PREFIX]}-{self._session_id}"
        queue_consumer = RabbitMQMessageMiddlewareQueue(rabbitmq_host, queue_name)
        self._mom_output_builders_connection = queue_consumer

    def __init__(
        self,
        client_socket: socket.socket,
        rabbitmq_host: str,
        cleaners_data: dict,
        output_builders_data: dict,
    ) -> None:
        self._pid = multiprocessing.current_process().pid
        self._client_socket = client_socket
        self._session_id = uuid.uuid4().hex

        self._set_as_not_running()
        signal.signal(signal.SIGTERM, self._sigterm_signal_handler)

        self._rabbitmq_host = rabbitmq_host

        self._init_cleaners_data(cleaners_data)
        self._init_output_builders_data(output_builders_data)

        self._init_client_data_batch_stats()
        self._init_output_builders_data_stats()

        self._init_mom_producers(rabbitmq_host)
        self._init_mom_consumers(rabbitmq_host)

        self._temp_buffer = b""

    # ============================== PRIVATE - ACCESSING ============================== #

    def _is_running(self) -> bool:
        return self._process_running

    def _set_as_not_running(self) -> None:
        self._process_running = False

    def _set_as_running(self) -> None:
        self._process_running = True

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        logging.info(
            f"action: sigterm_signal_handler | result: in_progress | session_id: {self._session_id}"
        )

        self._set_as_not_running()

        self._client_socket.close()
        logging.debug(
            f"action: sigterm_client_socket_close | result: success | session_id: {self._session_id}"
        )

        self._mom_output_builders_connection.stop_consuming()
        logging.debug(
            f"action: sigterm_mom_stop_consuming | result: success | session_id: {self._session_id}"
        )

        logging.info(
            f"action: sigterm_signal_handler | result: success | session_id: {self._session_id}"
        )

    # ============================== PRIVATE - SOCKET SEND/RECEIVE MESSAGES ============================== #

    def _socket_send_message(self, socket: socket.socket, message: str) -> None:
        logging.debug(
            f"action: send_message | result: in_progress | msg: {message} | session_id: {self._session_id}"
        )

        socket.sendall(message.encode("utf-8"))

        logging.debug(
            f"action: send_message | result: success |  msg: {message} | session_id: {self._session_id}"
        )

    def _socket_receive_message(self, socket: socket.socket) -> str:
        logging.debug(
            f"action: receive_message | result: in_progress | session_id: {self._session_id}"
        )

        buffsize = constants.KiB
        bytes_received = self._temp_buffer
        self._temp_buffer = b""

        all_data_received = False
        while not all_data_received:
            chunk = socket.recv(buffsize)
            if len(chunk) == 0:
                logging.error(
                    f"action: receive_message | result: fail | error: unexpected disconnection | session_id: {self._session_id}",
                )
                raise OSError(
                    f"Unexpected disconnection of the client | session_id: {self._session_id}"
                )

            logging.debug(
                f"action: receive_chunk | result: success | chunk size: {len(chunk)} | session_id: {self._session_id}"
            )
            if chunk.endswith(communication_protocol.MSG_END_DELIMITER.encode("utf-8")):
                all_data_received = True

            if communication_protocol.MSG_END_DELIMITER.encode("utf-8") in chunk:
                index = chunk.rindex(
                    communication_protocol.MSG_END_DELIMITER.encode("utf-8")
                )
                bytes_received += chunk[
                    : index + len(communication_protocol.MSG_END_DELIMITER)
                ]
                self._temp_buffer = chunk[
                    index + len(communication_protocol.MSG_END_DELIMITER) :
                ]
                all_data_received = True
            else:
                bytes_received += chunk

        message = bytes_received.decode("utf-8")
        logging.debug(
            f"action: receive_message | result: success | msg: {message} | session_id: {self._session_id}"
        )
        return message

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_to_next(self, data_type: str, message: str) -> None:
        current_worker_id = self._cleaners_data[data_type]["current_worker_id"]

        mom_producers = self._mom_cleaners_connections[data_type]
        mom_producer: RabbitMQMessageMiddlewareQueue = mom_producers[current_worker_id]
        mom_producer.send(message)

        current_worker_id += 1

        workers_amount = self._cleaners_data[data_type][constants.WORKERS_AMOUNT]
        if current_worker_id == workers_amount:
            current_worker_id = 0
        self._cleaners_data[data_type]["current_worker_id"] = current_worker_id

    # ============================== PRIVATE - RECEIVE CLIENT HANDSHAKE ============================== #

    def _send_client_handshake_message(
        self, client_socket: socket.socket, client_id: str
    ) -> None:
        handshake_response_message = communication_protocol.encode_handshake_message(
            self._session_id, client_id
        )
        self._socket_send_message(client_socket, handshake_response_message)
        logging.info(
            f"action: handshake_response_sent | result: success | client_id: {client_id} | session_id: {self._session_id}"
        )

    def _accept_client_handshake_message(self, client_socket: socket.socket) -> None:
        received_message = self._socket_receive_message(client_socket)
        (client_id, payload) = communication_protocol.decode_handshake_message(
            received_message
        )
        if payload != communication_protocol.ALL_QUERIES:
            raise ValueError(
                f"Invalid handshake payload received from client: {payload} | session_id: {self._session_id}"
            )
        logging.info(
            f"action: handshake_received | result: success | client_id: {client_id} | session_id: {self._session_id}"
        )

        self._send_client_handshake_message(client_socket, client_id)

    # ============================== PRIVATE - RECEIVE CLIENT DATA ============================== #

    def _handle_data_batch_message(self, message: str) -> None:
        data_type = communication_protocol.get_message_type(message)
        self._mom_send_message_to_next(data_type, message)

    def _handle_data_batch_eof_message(self, message: str) -> None:
        data_type = communication_protocol.decode_eof_message(message)
        if data_type not in self._client_data_batch_completed:
            raise ValueError(
                f'Invalid EOF message type received from client "{data_type}"'
            )
        self._client_data_batch_completed[data_type] = True
        logging.info(
            f"action: {data_type}_eof_received | result: success | session_id: {self._session_id}"
        )

        for mom_producer in self._mom_cleaners_connections[data_type]:
            mom_producer.send(message)

    def _handle_client_message(self, message: str) -> None:
        message_type = communication_protocol.get_message_type(message)
        match message_type:
            case (
                communication_protocol.MENU_ITEMS_BATCH_MSG_TYPE
                | communication_protocol.STORES_BATCH_MSG_TYPE
                | communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE
                | communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE
                | communication_protocol.USERS_BATCH_MSG_TYPE
            ):
                self._handle_data_batch_message(message)
            case communication_protocol.EOF:
                self._handle_data_batch_eof_message(message)
            case _:
                raise ValueError(
                    f'Invalid message type received from client "{message_type}" | session_id: {self._session_id}'
                )

    def _with_each_message_do(
        self,
        received_message: str,
        callback: Callable,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        messages = received_message.split(communication_protocol.MSG_END_DELIMITER)
        for message in messages:
            if not self._is_running():
                break
            if message == "":
                continue
            message += communication_protocol.MSG_END_DELIMITER
            callback(message, *args, **kwargs)

    def _receive_all_data_from_client(self, client_socket: socket.socket) -> None:
        while not all(self._client_data_batch_completed.values()):
            if not self._is_running():
                return

            received_message = self._socket_receive_message(client_socket)
            self._with_each_message_do(
                received_message,
                self._handle_client_message,
            )

        logging.info(
            f"action: all_data_received | result: success | session_id: {self._session_id}"
        )

    # ============================== PRIVATE - RECEIVE RESULTS ============================== #

    def _all_eof_received_from_output_builders(self) -> bool:
        for data_type, eof_received in self._output_builders_eof_received.items():
            workers_amount = self._output_builders_data[data_type][
                constants.WORKERS_AMOUNT
            ]
            if eof_received < workers_amount:
                return False
        return True

    def _handle_query_result_batch_message(
        self, client_socket: socket.socket, message: str
    ) -> None:
        self._socket_send_message(client_socket, message)

    def _handle_query_result_eof_message(
        self, client_socket: socket.socket, message: str
    ) -> None:
        data_type = communication_protocol.decode_eof_message(message)
        if data_type not in self._output_builders_eof_received:
            raise ValueError(
                f'Invalid EOF message type received from output builder "{data_type}" | session_id: {self._session_id}'
            )
        self._output_builders_eof_received[data_type] += 1
        logging.info(
            f"action: eof_{data_type}_result_received | result: success | session_id: {self._session_id}"
        )

        workers_amount = self._output_builders_data[data_type][constants.WORKERS_AMOUNT]
        if self._output_builders_eof_received[data_type] == workers_amount:
            self._socket_send_message(client_socket, message)
            logging.info(
                f"action: eof_{data_type}_results_to_client_sent | result: success | session_id: {self._session_id}"
            )

    def _handle_output_builder_message(self, client_socket: socket.socket) -> Callable:
        def _on_message_callback(message_as_bytes: bytes) -> None:
            message = message_as_bytes.decode("utf-8")
            message_type = communication_protocol.get_message_type(message)
            match message_type:
                case (
                    communication_protocol.QUERY_RESULT_1X_MSG_TYPE
                    | communication_protocol.QUERY_RESULT_21_MSG_TYPE
                    | communication_protocol.QUERY_RESULT_22_MSG_TYPE
                    | communication_protocol.QUERY_RESULT_3X_MSG_TYPE
                    | communication_protocol.QUERY_RESULT_4X_MSG_TYPE
                ):
                    self._handle_query_result_batch_message(client_socket, message)
                case communication_protocol.EOF:
                    self._handle_query_result_eof_message(client_socket, message)
                case _:
                    raise ValueError(
                        f'Invalid message type received from client "{message_type}" | session_id: {self._session_id}'
                    )

            if self._all_eof_received_from_output_builders():
                self._mom_output_builders_connection.stop_consuming()
                logging.info(
                    "action: all_results_received | result: success | session_id: {self._session_id}"
                )

        return _on_message_callback

    def _receive_all_query_results_from_output_builders(
        self, client_socket: socket.socket
    ) -> None:
        self._mom_output_builders_connection.start_consuming(
            self._handle_output_builder_message(client_socket)
        )

    # ============================== PRIVATE - HANDLE CLIENT CONNECTION ============================== #

    def _handle_client_connection(self, client_socket: socket.socket) -> None:
        self._accept_client_handshake_message(client_socket)
        self._receive_all_data_from_client(client_socket)
        self._receive_all_query_results_from_output_builders(client_socket)

    # ============================== PRIVATE - RUN ============================== #

    def _run(self) -> None:
        self._set_as_running()
        logging.info(
            "action: client_session_handler_running | result: success | session_id: {self._session_id}"
        )

        self._handle_client_connection(self._client_socket)

    def _close_all_mom_connections(self) -> None:
        for mom_cleaner_connections in self._mom_cleaners_connections.values():
            for mom_cleaner_connection in mom_cleaner_connections:
                mom_cleaner_connection.delete()
                mom_cleaner_connection.close()
                logging.debug(
                    f"action: mom_cleaner_connection_close | result: success | session_id: {self._session_id}"
                )

        self._mom_output_builders_connection.delete()
        self._mom_output_builders_connection.close()
        logging.debug(
            f"action: mom_output_builder_connection_close | result: success | session_id: {self._session_id}"
        )

    def _ensure_connections_close_after_doing(self, callback: Callable) -> None:
        try:
            callback()
        except Exception as e:
            logging.error(
                f"action: client_session_handler_run | result: fail | error: {e}"
            )
            raise e
        finally:
            self._client_socket.close()
            logging.debug(
                f"action: client_socket_close | result: success | session_id: {self._session_id}"
            )

            self._close_all_mom_connections()
            logging.info(
                f"action: all_mom_connections_close | result: success | session_id: {self._session_id}"
            )

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        logging.info(
            f"action: client_session_handler_startup | result: success | session_id: {self._session_id}"
        )

        self._ensure_connections_close_after_doing(self._run)

        logging.info(
            f"action: client_session_handler_shutdown | result: success | session_id: {self._session_id}"
        )
