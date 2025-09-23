import logging
import signal
import socket
from typing import Any

from shared import communication_protocol, constants


class Client:
    def __init__(
        self,
        client_id: int,
        server_host: str,
        server_port: int,
        data_path: str,
        batch_max_kib: int,
    ):
        self._client_id = client_id

        self._server_host = server_host
        self._server_port = server_port

        self._data_path = data_path
        self._batch_size = batch_max_kib

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
            if chunk.endswith(communication_protocol.END_MSG_DELIMITER.encode("utf-8")):
                all_data_received = True

            bytes_received += chunk

        message = bytes_received.decode("utf-8")
        logging.debug(f"action: receive_message | result: success | msg: {message}")
        return message

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        logging.info("action: client_startup | result: success")

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server_socket.connect((self._server_host, self._server_port))
            self.__send_message(server_socket, "[Hola mundo!]")
        except Exception as e:
            logging.error(f"action: client_run | result: fail | error: {e}")
            raise e
        finally:
            server_socket.close()
            logging.debug("action: server_socket_close | result: success")

        logging.info("action: client_shutdown | result: success")
