from typing import Any

from controllers.cleaners.cleaner import Cleaner
from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_exchange import (
    RabbitMQMessageMiddlewareExchange,
)


class StoresCleaner(Cleaner):

    # ============================== INITIALIZE ============================== #

    def _build_mom_producer_using(
        self, rabbitmq_host: str, producers_config: dict[str, Any], producer_id: int
    ) -> MessageMiddleware:
        exchange_name = producers_config["exchange_name_prefix"]
        routing_key = f"{producers_config["routing_key_prefix"]}.{producer_id}"
        return RabbitMQMessageMiddlewareExchange(
            host=rabbitmq_host,
            exchange_name=exchange_name,
            route_keys=[routing_key],
        )

    # ============================== PRIVATE - ACCESSING ============================== #

    def _columns_to_keep(self) -> list[str]:
        return [
            "store_id",
            "store_name",
        ]

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_to_next(self, message: str) -> None:
        mom_producer = self._mom_producers[self._current_producer_id]
        mom_producer.send(message)

        self._current_producer_id += 1
        if self._current_producer_id >= len(self._mom_producers):
            self._current_producer_id = 0
