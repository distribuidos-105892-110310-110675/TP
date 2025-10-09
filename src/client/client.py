import logging
import signal
import socket
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Callable

from shared import communication_protocol, constants, shell_cmd


class Client:

    # ============================== INITIALIZE ============================== #

    def __init__(
        self,
        client_id: int,
        server_host: str,
        server_port: int,
        data_path: str,
        results_path: str,
        batch_max_size: int,
    ):
        self._client_id = client_id
        self._session_id = ""

        self._server_host = server_host
        self._server_port = server_port

        self._data_path = Path(data_path)

        self._output_path = Path(results_path) / constants.QRS_FOLDER_NAME
        self._output_path.mkdir(parents=True, exist_ok=True)
        shell_cmd.shell_silent(f"rm -f {self._output_path}/*")

        self._batch_max_size = batch_max_size

        self._client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self._set_client_as_not_running()
        signal.signal(signal.SIGTERM, self._sigterm_signal_handler)

        self._temp_buffer = b""

    # ============================== PRIVATE - RUNNING ============================== #

    def _is_running(self) -> bool:
        return self._client_running == True

    def _set_client_as_not_running(self) -> None:
        self._client_running = False

    def _set_client_as_running(self) -> None:
        self._client_running = True

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        logging.info("action: sigterm_signal_handler | result: in_progress")

        self._set_client_as_not_running()

        self._client_socket.close()
        logging.debug("action: sigterm_client_socket_close | result: success")

        logging.info("action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - SEND/RECEIVE MESSAGES ============================== #

    def _socket_send_message(self, socket: socket.socket, message: str) -> None:
        logging.debug(f"action: send_message | result: in_progress | msg: {message}")

        socket.sendall(message.encode("utf-8"))

        logging.debug(f"action: send_message | result: success |  msg: {message}")

    def _socket_receive_message(self, socket: socket.socket) -> str:
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
                raise OSError("Unexpected disconnection of the server")

            logging.debug(
                f"action: receive_chunk | result: success | chunk size: {len(chunk)}"
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
        logging.debug(f"action: receive_message | result: success | msg: {message}")
        return message

    # ============================== PRIVATE - READ CSV ============================== #

    def _parse_row_from_line(
        self, column_names: list[str], line: str
    ) -> dict[str, str]:
        row: dict[str, str] = {}

        fields = line.split(",")
        for i, column_name in enumerate(column_names):
            row[column_name] = fields[i]

        return row

    def _read_next_batch_from_file(
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

            row = self._parse_row_from_line(column_names, line)
            batch.append(row)
            batch_size += 1

        return batch

    # ============================== PRIVATE - PATHS SUPPORT ============================== #

    def _assert_is_file(self, path: Path) -> None:
        if not path.exists() or not path.is_file():
            raise ValueError(f"Data path error: {path} is not a file")

    def _assert_is_dir(self, path: Path) -> None:
        if not path.exists() or not path.is_dir():
            raise ValueError(f"Data path error: {path} is not a folder")

    def _folder_path(self, folder_name: str) -> Path:
        folder_path = self._data_path / folder_name
        self._assert_is_dir(folder_path)
        return folder_path

    # ============================== PRIVATE - SEND/RECV HANDSHAKE ============================== #

    def _send_handshake_message(self) -> None:
        handshake_message = communication_protocol.encode_handshake_message(
            str(self._client_id), communication_protocol.ALL_QUERIES
        )
        self._socket_send_message(self._client_socket, handshake_message)
        logging.info("action: send_handshake | result: success")

    def _receive_handshake_ack_message(self) -> None:
        received_message = self._socket_receive_message(self._client_socket)
        self._session_id, client_id = communication_protocol.decode_handshake_message(
            received_message
        )
        if client_id != str(self._client_id):
            raise ValueError(
                f"Handshake ACK message error: expected client_id {self._client_id}, received {client_id}"
            )

        logging.info(
            f"action: receive_handshake_ack | result: success | session_id: {self._session_id}"
        )

    # ============================== PRIVATE - SEND DATA ============================== #

    def _send_data_from_file_using_batchs(
        self,
        folder_name: str,
        file: TextIOWrapper,
        encoding_callback: Callable,
    ) -> None:
        column_names_line = file.readline().strip()
        column_names = column_names_line.split(",")

        batch = self._read_next_batch_from_file(file, column_names)
        while len(batch) != 0 and self._is_running():
            logging.debug(f"action: {folder_name}_batch | result: in_progress")
            message = encoding_callback(self._session_id, batch)
            self._socket_send_message(self._client_socket, message)
            logging.debug(f"action: {folder_name}_batch | result: success")

            batch = self._read_next_batch_from_file(file, column_names)

    def _send_data_from_all_files_using_batchs(
        self,
        folder_name: str,
        message_type: str,
        encoding_callback: Callable,
    ) -> None:
        for file_path in self._folder_path(folder_name).iterdir():
            if not file_path.name.lower().endswith(".csv"):
                logging.warning(
                    f"action: {folder_name}_file_skip | result: success | file: {file_path} | reason: not_csv"
                )
                continue
            self._assert_is_file(file_path)
            csv_file = open(file_path, "r", encoding="utf-8", buffering=constants.KiB)
            try:
                self._send_data_from_file_using_batchs(
                    folder_name,
                    csv_file,
                    encoding_callback,
                )
            finally:
                csv_file.close()
                logging.debug(
                    f"action: {folder_name}_file_close | result: success | file: {file_path}"
                )

        eof_message = communication_protocol.encode_eof_message(
            self._session_id, message_type
        )
        self._socket_send_message(self._client_socket, eof_message)

    def _send_all_menu_items(self) -> None:
        self._send_data_from_all_files_using_batchs(
            constants.MIT_FOLDER_NAME,
            communication_protocol.MENU_ITEMS_BATCH_MSG_TYPE,
            communication_protocol.encode_menu_items_batch_message,
        )

    def _send_all_stores(self) -> None:
        self._send_data_from_all_files_using_batchs(
            constants.STR_FOLDER_NAME,
            communication_protocol.STORES_BATCH_MSG_TYPE,
            communication_protocol.encode_stores_batch_message,
        )

    def _send_all_transaction_items(self) -> None:
        self._send_data_from_all_files_using_batchs(
            constants.TIT_FOLDER_NAME,
            communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE,
            communication_protocol.encode_transaction_items_batch_message,
        )

    def _send_all_transactions(self) -> None:
        self._send_data_from_all_files_using_batchs(
            constants.TRN_FOLDER_NAME,
            communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE,
            communication_protocol.encode_transactions_batch_message,
        )

    def _send_all_users(self) -> None:
        self._send_data_from_all_files_using_batchs(
            constants.USR_FOLDER_NAME,
            communication_protocol.USERS_BATCH_MSG_TYPE,
            communication_protocol.encode_users_batch_message,
        )

    def _send_all_data(self) -> None:
        # WARNING: do not modify order
        self._send_all_menu_items()
        self._send_all_stores()
        self._send_all_transactions()
        self._send_all_transaction_items()
        self._send_all_users()
        logging.info("action: all_data_sent | result: success")

    # ============================== PRIVATE - SEND DATA ============================== #

    def _handle_query_result_eof_message(
        self, message: str, all_eof_received: dict
    ) -> None:
        data_type = communication_protocol.decode_eof_message(message)
        if data_type not in all_eof_received:
            raise ValueError(f"Unknown EOF message type {data_type}")

        all_eof_received[data_type] = True
        logging.info(f"action: eof_{data_type}_receive_query_result | result: success")

    def _handle_query_result_message(self, message: str, message_type: str) -> None:
        logging.debug(f"action: {message_type}_receive_query_result | result: success")
        file_name = f"client_{self._client_id}_{message_type}_result.txt"
        for item_batch in communication_protocol.decode_batch_message(message):
            shell_cmd.shell_silent(
                f"echo '{",".join(item_batch.values())}' >> {self._output_path / file_name}"
            )
            logging.debug(
                f"action: {message_type}_save_query_result | result: success | file: {file_name}",
            )

    def _handle_server_message(self, message: str, all_eof_received: dict) -> None:
        message_type = communication_protocol.get_message_type(message)
        match message_type:
            case (
                communication_protocol.QUERY_RESULT_1X_MSG_TYPE
                | communication_protocol.QUERY_RESULT_21_MSG_TYPE
                | communication_protocol.QUERY_RESULT_22_MSG_TYPE
                | communication_protocol.QUERY_RESULT_3X_MSG_TYPE
                | communication_protocol.QUERY_RESULT_4X_MSG_TYPE
            ):
                self._handle_query_result_message(message, message_type)
            case communication_protocol.EOF:
                self._handle_query_result_eof_message(message, all_eof_received)
            case _:
                raise ValueError(
                    f'Invalid message type received from server "{message_type}"'
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

    def _receive_all_query_results_from_server(self) -> None:
        all_eof_received = {
            constants.QUERY_RESULT_1X: False,
            constants.QUERY_RESULT_21: False,
            constants.QUERY_RESULT_22: False,
            constants.QUERY_RESULT_3X: False,
            constants.QUERY_RESULT_4X: False,
        }

        while not all(all_eof_received.values()):
            if not self._is_running():
                return

            received_message = self._socket_receive_message(self._client_socket)
            self._with_each_message_do(
                received_message,
                self._handle_server_message,
                all_eof_received,
            )

        logging.info("action: all_query_results_received | result: success")

    # ============================== PRIVATE - HANDLE SERVER CONNECTION ============================== #

    def _handle_server_connection(self) -> None:
        self._send_handshake_message()
        self._receive_handshake_ack_message()

        self._send_all_data()

        # TODO: we should check that each message has the session id
        self._receive_all_query_results_from_server()

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        logging.info("action: client_startup | result: success")

        self._set_client_as_running()

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._client_socket.connect((self._server_host, self._server_port))
            self._handle_server_connection()
        except Exception as e:
            logging.error(f"action: client_run | result: fail | error: {e}")
            raise e
        finally:
            server_socket.close()
            logging.debug("action: server_socket_close | result: success")

        logging.info("action: client_shutdown | result: success")
