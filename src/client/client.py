import logging
import signal
import socket
from io import TextIOWrapper
from pathlib import Path
from typing import Any

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

        signal.signal(signal.SIGTERM, self.__sigterm_signal_handler)

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def __sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        logging.info("action: sigterm_signal_handler | result: in_progress")

        # self.__set_server_as_not_running()

        # self._server_socket.close()
        logging.debug("action: sigterm_server_socket_close | result: success")

        logging.info("action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - SEND/RECEIVE MESSAGES ============================== #

    def __send_message(self, client_connection: socket.socket, message: str) -> None:
        logging.debug(f"action: send_message | result: in_progress | msg: {message}")

        client_connection.sendall(message.encode("utf-8"))

        logging.debug(f"action: send_message | result: success |  msg: {message}")

    def __receive_message(self, client_connection: socket.socket) -> str:
        logging.debug(f"action: receive_message | result: in_progress")

        buffsize = constants.KiB
        bytes_received = b""

        all_data_received = False
        while not all_data_received:
            chunk = client_connection.recv(buffsize)
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

    # ============================== PRIVATE - SEND MENU ITEMS ============================== #

    def __send_all_menu_items_using_batchs(
        self, server_socket: socket.socket, file: TextIOWrapper
    ) -> None:
        column_names_line = file.readline().strip()
        column_names = column_names_line.split(",")

        batch = self.__read_next_batch_from_file(file, column_names)
        while len(batch) != 0:
            logging.debug(f"action: send_menu_items_batch | result: in_progress")
            message = communication_protocol.encode_menu_items_batch_message(batch)
            self.__send_message(server_socket, message)
            logging.debug(f"action: send_menu_items_batch | result: success")

            batch = self.__read_next_batch_from_file(file, column_names)

    def __send_menu_items(self, server_socket: socket.socket) -> None:
        folder_path = self._data_path / MENU_ITEMS_FOLDER_NAME
        if not folder_path.exists() or not folder_path.is_dir():
            raise ValueError(f"Data path error: {folder_path} is not a folder")

        for file_path in folder_path.iterdir():
            if not file_path.is_file():
                raise ValueError(f"Data path error: {file_path} is not a file")

            csv_file = open(file_path, "rt", newline="")
            try:
                self.__send_all_menu_items_using_batchs(server_socket, csv_file)
            finally:
                csv_file.close()
                logging.debug("action: menu_items_file_close | result: success")

    # ============================== PRIVATE - SEND STORES ============================== #

    # ============================== PRIVATE - SEND TRANSACTION ITEMS ============================== #

    # ============================== PRIVATE - SEND TRANSACTIONS ============================== #

    # ============================== PRIVATE - SEND USERS ============================== #

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        logging.info("action: client_startup | result: success")

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server_socket.connect((self._server_host, self._server_port))
            self.__send_menu_items(server_socket)
        except Exception as e:
            logging.error(f"action: client_run | result: fail | error: {e}")
            raise e
        finally:
            server_socket.close()
            logging.debug("action: server_socket_close | result: success")

        logging.info("action: client_shutdown | result: success")
