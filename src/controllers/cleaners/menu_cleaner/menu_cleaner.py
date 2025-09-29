import logging
import signal
from typing import Any


class MenuCleaner:

    def __init__(
        self,
        cleaner_id: int,
        rabbitmq_host: str,
        cleaner_data_queue: str,
    ) -> None:
        self.cleaner_id = cleaner_id
        self.rabbitmq_host = rabbitmq_host

        signal.signal(signal.SIGTERM, self.__sigterm_signal_handler)

    # ============================== PRIVATE - ACCESSING ============================== #

    def __is_running(self) -> bool:
        return self._server_running

    def __set_controller_as_not_running(self) -> None:
        self._server_running = False

    def __set_controller_as_running(self) -> None:
        self._server_running = True

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def __sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        logging.info("action: sigterm_signal_handler | result: in_progress")

        self.__set_controller_as_not_running()

        logging.info("action: sigterm_signal_handler | result: success")

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        logging.info("action: cleaner_startup | result: success")

        logging.info("MenuCleaner is still running")

        logging.info("action: cleaner_shutdown | result: success")
