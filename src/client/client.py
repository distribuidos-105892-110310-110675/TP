import logging
import signal
import socket
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Callable

from shared import communication_protocol, constants

MENU_ITEMS_FOLDER_NAME = "menu_items"
STORES_FOLDER_NAME = "stores"
TRANSACTION_ITEMS_FOLDER_NAME = "transaction_items"
TRANSACTIONS_FOLDER_NAME = "transactions"
USERS_FOLDER_NAME = "users"


class Client:
    def __init__(
        self,
        client_id: int,
        server_host: str,
        server_port: int,
        data_path: str,
        batch_max_size: int,
    ):
        self._client_id = client_id

        self._server_host = server_host
        self._server_port = server_port

        self._data_path = Path(data_path)
        self._batch_max_size = batch_max_size

        self.__set_client_as_not_running()
        signal.signal(signal.SIGTERM, self.__sigterm_signal_handler)

    # ============================== PRIVATE - RUNNING ============================== #

    def __is_running(self) -> bool:
        return self._client_running == True

    def __set_client_as_not_running(self) -> None:
        self._client_running = False

    def __set_client_as_running(self) -> None:
        self._client_running = True

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def __sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        logging.info("action: sigterm_signal_handler | result: in_progress")

        self.__set_client_as_not_running()

        logging.info("action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - SEND/RECEIVE MESSAGES ============================== #

    def __socket_send_message(self, socket: socket.socket, message: str) -> None:
        logging.debug(f"action: send_message | result: in_progress | msg: {message}")

        socket.sendall(message.encode("utf-8"))

        logging.debug(f"action: send_message | result: success |  msg: {message}")

    def __socket_receive_message(self, socket: socket.socket) -> str:
        logging.debug(f"action: receive_message | result: in_progress")

        buffsize = constants.KiB
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

    # ============================== PRIVATE - READ CSV ============================== #

    def __parse_row_from_line(
        self, column_names: list[str], line: str
    ) -> dict[str, str]:
        row: dict[str, str] = {}

        fields = line.split(",")
        for i, column_name in enumerate(column_names):
            row[column_name] = fields[i]

        return row

    def __read_next_batch_from_file(
        self, file: TextIOWrapper, column_names: list[str]
    ) -> list[dict[str, str]]:
        batch: list[dict[str, str]] = []

        batch_size = 0
        eof_reached = False

        while not eof_reached and batch_size < self._batch_max_size:
            row = {}

            line = file.readline().strip()
            if not line:
                eof_reached = True
                continue

            row = self.__parse_row_from_line(column_names, line)
            batch.append(row)
            batch_size += 1

        return batch

    # ============================== PRIVATE - PATHS SUPPORT ============================== #

    def __assert_is_file(self, path: Path) -> None:
        if not path.exists() or not path.is_file():
            raise ValueError(f"Data path error: {path} is not a file")

    def __assert_is_dir(self, path: Path) -> None:
        if not path.exists() or not path.is_dir():
            raise ValueError(f"Data path error: {path} is not a folder")

    def __folder_path(self, folder_name: str) -> Path:
        folder_path = self._data_path / folder_name
        self.__assert_is_dir(folder_path)
        return folder_path

    # ============================== PRIVATE - SEND DATA ============================== #

    def __send_all_data_using_batchs(
        self,
        socket: socket.socket,
        folder_name: str,
        file: TextIOWrapper,
        encoding_callback: Callable,
    ) -> None:
        column_names_line = file.readline().strip()
        column_names = column_names_line.split(",")

        batch = self.__read_next_batch_from_file(file, column_names)
        while len(batch) != 0 and self.__is_running():
            logging.debug(f"action: {folder_name}_batch | result: in_progress")
            message = encoding_callback(batch)
            self.__socket_send_message(socket, message)
            logging.debug(f"action: {folder_name}_batch | result: success")

            batch = self.__read_next_batch_from_file(file, column_names)

    def __send_data(
        self,
        socket: socket.socket,
        folder_name: str,
        message_type: str,
        encoding_callback: Callable,
    ) -> None:
        for file_path in self.__folder_path(folder_name).iterdir():
            self.__assert_is_file(file_path)
            csv_file = open(file_path, "rt", newline="")
            try:
                self.__send_all_data_using_batchs(
                    socket,
                    folder_name,
                    csv_file,
                    encoding_callback,
                )
            finally:
                csv_file.close()
                logging.debug(
                    f"action: {folder_name}_file_close | result: success | file: {file_path}"
                )

        eof_message = communication_protocol.encode_eof_message(message_type)
        self.__socket_send_message(socket, eof_message)

    def __send_menu_items(self, server_socket: socket.socket) -> None:
        self.__send_data(
            server_socket,
            MENU_ITEMS_FOLDER_NAME,
            communication_protocol.MENU_ITEMS_BATCH_MSG_TYPE,
            communication_protocol.encode_menu_items_batch_message,
        )

    def __send_stores(self, server_socket: socket.socket) -> None:
        self.__send_data(
            server_socket,
            STORES_FOLDER_NAME,
            communication_protocol.STORES_BATCH_MSG_TYPE,
            communication_protocol.encode_stores_batch_message,
        )

    def __send_transaction_items(self, server_socket: socket.socket) -> None:
        self.__send_data(
            server_socket,
            TRANSACTION_ITEMS_FOLDER_NAME,
            communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE,
            communication_protocol.encode_transaction_items_batch_message,
        )

    def __send_transactions(self, server_socket: socket.socket) -> None:
        self.__send_data(
            server_socket,
            TRANSACTIONS_FOLDER_NAME,
            communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE,
            communication_protocol.encode_transactions_batch_message,
        )

    def __send_users(self, server_socket: socket.socket) -> None:
        self.__send_data(
            server_socket,
            USERS_FOLDER_NAME,
            communication_protocol.USERS_BATCH_MSG_TYPE,
            communication_protocol.encode_users_batch_message,
        )

    def __send_all_data(self, server_socket: socket.socket) -> None:
        # WARNING: do not modify order
        self.__send_menu_items(server_socket)
        self.__send_stores(server_socket)
        self.__send_transactions(server_socket)
        self.__send_transaction_items(server_socket)
        self.__send_users(server_socket)
        logging.info("action: all_data_sent | result: success")

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        logging.info("action: client_startup | result: success")

        self.__set_client_as_running()

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server_socket.connect((self._server_host, self._server_port))

            # send a message to identify the client
            # receive an ack message from the server

            self.__send_all_data(server_socket)

            # now we wait for a response from the server
            # this will be sent as batchs too with the result of each query

        except Exception as e:
            logging.error(f"action: client_run | result: fail | error: {e}")
            raise e
        finally:
            server_socket.close()
            logging.debug("action: server_socket_close | result: success")

        logging.info("action: client_shutdown | result: success")
