from typing import Any

from controllers.cleaners.cleaner import Cleaner
from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue


class TransactionsCleaner(Cleaner):

    # ============================== INITIALIZE ============================== #

    def _build_mom_producer_using(
        self, rabbitmq_host: str, producers_config: dict[str, Any], producer_id: int
    ) -> MessageMiddleware:
        queue_name_prefix = producers_config["queue_name_prefix"]
        queue_name = f"{queue_name_prefix}-{producer_id}"
        return RabbitMQMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_name)

    # ============================== PRIVATE - ACCESSING ============================== #

    def _columns_to_keep(self) -> list[str]:
        return [
            "created_at",
            "store_id",
            "final_amount",
            "transaction_id",
            "user_id",
        ]

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_to_next(self, message: str) -> None:
        mom_cleaned_data_producer = self._mom_producers[self._current_producer_id]
        mom_cleaned_data_producer.send(message)

        self._current_producer_id += 1
        if self._current_producer_id >= len(self._mom_producers):
            self._current_producer_id = 0
