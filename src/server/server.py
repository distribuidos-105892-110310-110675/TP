import logging
import signal
import socket
from typing import Any, Optional

from shared import communication_protocol, constants


class Server:

    # ============================== INITIALIZE ============================== #

    def __init__(self, port: int, listen_backlog: int) -> None:
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)

        self.__set_server_as_not_running()

        signal.signal(signal.SIGTERM, self.__sigterm_signal_handler)

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

    # ============================== PRIVATE - HANDLE CONNECTION ============================== #

    def __handle_client_connection(self, client_connection: socket.socket) -> None:
        message = self.__receive_message(client_connection)
        logging.info(
            f"action: handle_client_connection | result: success | msg: {message}"
        )

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        logging.info("action: server_startup | result: success")

        self.__set_server_as_running()
        try:
            while self.__is_running():
                client_connection = self.__accept_new_connection()
                if client_connection is None:
                    continue

                try:
                    self.__handle_client_connection(client_connection)
                except Exception as e:
                    client_connection.close()
                    logging.debug("action: client_connection_close | result: success")
                    raise e
        except Exception as e:
            logging.error(f"action: server_run | result: fail | error: {e}")
            raise e
        finally:
            self._server_socket.close()
            logging.debug("action: server_socket_close | result: success")

        logging.info("action: server_shutdown | result: success")
