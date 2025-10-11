import logging
import multiprocessing
import signal
import socket
from collections.abc import Callable
from typing import Any, Optional

from server.client_session_handler import ClientSessionHandler


class Server:

    # ============================== INITIALIZE ============================== #

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

        self._set_server_as_not_running()
        signal.signal(signal.SIGTERM, self._sigterm_signal_handler)
        signal.signal(signal.SIGCHLD, self._sigchld_signal_handler)

        self._rabbitmq_host = rabbitmq_host
        self._cleaners_data = cleaners_data
        self._output_builders_data = output_builders_data

        self._spawned_processes: list[multiprocessing.Process] = []

    # ============================== PRIVATE - ACCESSING ============================== #

    def _is_running(self) -> bool:
        return self._server_running

    def _set_server_as_not_running(self) -> None:
        self._server_running = False

    def _set_server_as_running(self) -> None:
        self._server_running = True

    # ============================== PRIVATE - HANDLE PROCESSES ============================== #

    def _terminate_all_processes(self) -> None:
        for process in self._spawned_processes:
            process.terminate()
            process.join()
            process.close()
            logging.debug(
                f"action: terminate_process | result: success | pid: {process.pid}"
            )
        self._spawned_processes.clear()
        logging.info("action: all_processes_terminate | result: success")

    def _check_spawned_processes(self) -> None:
        for process in self._spawned_processes:
            if process.is_alive():
                continue
            process.join()
            self._spawned_processes.remove(process)
            logging.info(f"action: process_join | result: success | pid: {process.pid}")

            if process.exitcode == 0:
                continue
            logging.error(
                f"action: process_error | result: error | pid: {process.pid} | exitcode: {process.exitcode}"
            )
            self._terminate_all_processes()
            raise Exception(
                f"Process {process.pid} exited with code {process.exitcode}"
            )

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _sigchld_signal_handler(self, signum: Any, frame: Any) -> None:
        logging.info("action: sigchld_signal_handler | result: in_progress")

        self._check_spawned_processes()

        logging.info("action: sigchld_signal_handler | result: success")

    def _sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        logging.info("action: sigterm_signal_handler | result: in_progress")

        self._set_server_as_not_running()

        self._server_socket.close()
        logging.debug("action: sigterm_server_socket_close | result: success")

        self._terminate_all_processes()

        logging.info("action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - ACCEPT CONNECTION ============================== #

    def _accept_new_connection(self) -> Optional[socket.socket]:
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

    # ============================== PRIVATE - HANDLE CLIENT CONNECTION ============================== #

    def _handle_client_connection(self, client_socket: socket.socket) -> None:
        ClientSessionHandler(
            client_socket,
            self._rabbitmq_host,
            self._cleaners_data,
            self._output_builders_data,
        ).run()

    def _handle_client_connection_spawning_process(
        self, client_socket: socket.socket
    ) -> None:
        process = multiprocessing.Process(
            target=self._handle_client_connection, args=(client_socket,)
        )
        process.start()
        self._spawned_processes.append(process)
        logging.info(
            f"action: spawn_client_connection_process | result: success | pid: {process.pid}"
        )

    # ============================== PRIVATE - RUN ============================== #

    def _run(self) -> None:
        self._set_server_as_running()

        while self._is_running():
            client_socket = self._accept_new_connection()
            if client_socket is None:
                continue

            self._handle_client_connection_spawning_process(client_socket)
            self._check_spawned_processes()

    def _ensure_connections_close_after_doing(self, callback: Callable) -> None:
        try:
            callback()
        except Exception as e:
            logging.error(f"action: server_run | result: fail | error: {e}")
            raise e
        finally:
            self._server_socket.close()
            logging.debug("action: server_socket_close | result: success")

            self._terminate_all_processes()

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        logging.info("action: server_startup | result: success")

        self._ensure_connections_close_after_doing(self._run)

        logging.info("action: server_shutdown | result: success")
