import logging
import signal
from abc import ABC, abstractmethod
from typing import Any, Callable


class Controller(ABC):

    # ============================== INITIALIZE ============================== #

    @abstractmethod
    def _init_mom_consumers(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> None:
        raise NotImplementedError("subclass resposability")

    @abstractmethod
    def _init_mom_producers(
        self,
        rabbitmq_host: str,
        producers_config: dict[str, Any],
    ) -> None:
        raise NotImplementedError("subclass resposability")

    def __init__(
        self,
        controller_id: int,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
        producers_config: dict[str, Any],
    ) -> None:
        self._controller_id = controller_id

        self._set_controller_as_not_running()
        signal.signal(signal.SIGTERM, self._sigterm_signal_handler)

        self._init_mom_consumers(rabbitmq_host, consumers_config)
        self._init_mom_producers(rabbitmq_host, producers_config)

    # ============================== PRIVATE - ACCESSING ============================== #

    def _is_running(self) -> bool:
        return self._controller_running

    def _set_controller_as_not_running(self) -> None:
        self._controller_running = False

    def _set_controller_as_running(self) -> None:
        self._controller_running = True

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    @abstractmethod
    def _mom_stop_consuming(self) -> None:
        raise NotImplementedError("subclass resposability")

    def _sigterm_signal_handler(self, signum: Any, frame: Any) -> None:
        logging.info("action: sigterm_signal_handler | result: in_progress")

        self._set_controller_as_not_running()
        self._mom_stop_consuming()

        logging.info("action: sigterm_signal_handler | result: success")

    # ============================== PRIVATE - RUN ============================== #

    def _run(self) -> None:
        self._set_controller_as_running()
        logging.info("action: controller_running | result: success")

    @abstractmethod
    def _close_all_mom_connections(self) -> None:
        raise NotImplementedError("subclass resposability")

    def _ensure_connections_close_after_doing(self, callback: Callable) -> None:
        try:
            callback()
        except Exception as e:
            logging.error(f"action: controller_run | result: fail | error: {e}")
            raise e
        finally:
            self._close_all_mom_connections()
            logging.info("action: all_mom_connections_close | result: success")

    # ============================== PUBLIC ============================== #

    def run(self) -> None:
        logging.info("action: controller_startup | result: success")

        self._ensure_connections_close_after_doing(self._run)

        logging.info("action: controller_shutdown | result: success")
