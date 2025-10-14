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

        self._set_server_as_stopped()
        signal.signal(signal.SIGTERM, self._sigterm_signal_handler)
        signal.signal(signal.SIGCHLD, self._sigchld_signal_handler)

        self._rabbitmq_host = rabbitmq_host
        self._cleaners_data = cleaners_data
        self._output_builders_data = output_builders_data

        self._spawned_processes: list[multiprocessing.Process] = []

    # ============================== PRIVATE - LOGGING ============================== #

    def _log_debug(self, text: str) -> None:
        logging.debug(f"{text} | pid: main_process")

    def _log_info(self, text: str) -> None:
        logging.info(f"{text} | pid: main_process")

    def _log_error(self, text: str) -> None:
        logging.error(f"{text} | pid: main_process")

    # ============================== PRIVATE - ACCESSING ============================== #

    def _is_running(self) -> bool:
        return self._server_running

    def _set_server_as_stopped(self) -> None:
        self._server_running = False

    def _set_server_as_running(self) -> None:
        self._server_running = True

    # ============================== PRIVATE - HANDLE PROCESSES ============================== #

    def _terminate_all_processes(self) -> None:
        for process in self._spawned_processes:
            if process.is_alive():
                process.terminate()
                logging.debug(
                    f"action: terminate_process | result: success | pid: {process.pid}"
                )
        logging.info("action: all_processes_terminate | result: success")

    def _join_all_processes(self) -> None:
        for process in self._spawned_processes:
            process.join()
            pid = process.pid
            exitcode = process.exitcode
            logging.info(
                f"action: join_process | result: success | pid: {pid} | exitcode: {exitcode}"
            )

    def _close_all_processes(self) -> list[tuple[int, int]]:
        uncaught_exceptions = []
        for process in self._spawned_processes:
            pid = process.pid
            exitcode = process.exitcode
            if exitcode != 0:
                uncaught_exceptions.append((pid, exitcode))

            process.close()
            logging.debug(f"action: close_process | result: success | pid: {pid}")
        return uncaught_exceptions

    def _join_non_alive_processes(self) -> None:
        for process in self._spawned_processes:
            if not process.is_alive():
                process.join()
                pid = process.pid
                exitcode = process.exitcode
                logging.info(
                    f"action: join_process | result: success | pid: {pid} | exitcode: {exitcode}"
                )

                if exitcode != 0:
                    self._stop()
                else:
                    process.close()
                    self._spawned_processes.remove(process)
                    logging.info(
                        f"action: close_process | result: success | pid: {pid}"
                    )

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _sigchld_signal_handler(self, signum: Any, frame: Any) -> None:
        logging.info("action: sigchld_signal_handler | result: in_progress")

        self._join_non_alive_processes()

        logging.info("action: sigchld_signal_handler | result: success")

    def _stop(self) -> None:
        self._server_socket.close()
        logging.debug("action: server_socket_close | result: success")

    def _sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        logging.info("action: sigterm_signal_handler | result: in_progress")

        self._set_server_as_stopped()
        self._stop()

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

    def _deny_exitcode_with_error(
        self, exitcode: Optional[int], pid: Optional[int]
    ) -> None:
        if exitcode != 0:
            logging.error(
                f"action: process_error | result: error | pid: {pid} | exitcode: {exitcode}"
            )
            raise Exception(f"Process {pid} exited with code {exitcode}")

    def _close_all(self) -> None:
        self._server_socket.close()
        logging.debug("action: server_socket_close | result: success")

        self._terminate_all_processes()
        self._join_all_processes()
        uncaught_exceptions = self._close_all_processes()
        if len(uncaught_exceptions) != 0:
            uncaught_exceptions_as_str = ", ".join(
                [
                    f"(pid: {pid}, exitcode: {exitcode})"
                    for pid, exitcode in uncaught_exceptions
                ]
            )
            raise Exception(
                f"Some processes exited with errors: {uncaught_exceptions_as_str}"
            )

    def _ensure_connections_close_after_doing(self, callback: Callable) -> None:
        try:
            callback()
        except Exception as e:
            logging.error(f"action: server_run | result: fail | error: {e}")
            raise e
        finally:
            self._close_all()
            logging.info("action: close_all | result: success")

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        logging.info("action: server_startup | result: success")

        self._ensure_connections_close_after_doing(self._run)

        logging.info("action: server_shutdown | result: success")
