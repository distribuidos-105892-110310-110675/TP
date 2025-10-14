import logging
from typing import Any

from controllers.filters.shared.filter import Filter
from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_exchange import (
    RabbitMQMessageMiddlewareExchange,
)
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol


class FilterTransactionsByYear(Filter):

    # ============================== INITIALIZE ============================== #

    def _build_mom_consumer_using(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> MessageMiddleware:
        queue_name_prefix = consumers_config["queue_name_prefix"]
        queue_name = f"{queue_name_prefix}-{self._controller_id}"
        return RabbitMQMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_name)

    def _build_mom_producer_using(
        self,
        rabbitmq_host: str,
        producers_config: dict[str, Any],
        producer_id: int,
    ) -> MessageMiddleware:
        exchange_name = producers_config["exchange_name_prefix"]
        routing_key = f"{producers_config["routing_key_prefix"]}.{producer_id}"
        return RabbitMQMessageMiddlewareExchange(
            host=rabbitmq_host,
            exchange_name=exchange_name,
            route_keys=[routing_key],
        )

    def __init__(
        self,
        controller_id: int,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
        producers_config: dict[str, Any],
        years_to_keep: list[int],
    ) -> None:
        super().__init__(
            controller_id,
            rabbitmq_host,
            consumers_config,
            producers_config,
        )

        self._years_to_keep = set(years_to_keep)

    # ============================== PRIVATE - TRANSFORM DATA ============================== #

    def _should_be_included(self, batch_item: dict[str, str]) -> bool:
        created_at = batch_item["created_at"]
        date = created_at.split(" ")[0]
        year = int(date.split("-")[0])

        return year in self._years_to_keep

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_to_next(self, message: str) -> None:
        user_batchs_by_hash: dict[int, list] = {}

        message_type = communication_protocol.get_message_type(message)
        session_id = communication_protocol.get_message_session_id(message)
        for batch_item in communication_protocol.decode_batch_message(message):
            if batch_item["user_id"] == "":
                # [IMPORTANT] If user_id is empty, the hash will fail
                # but we are going to assign it to the first reducer anyway
                key = 0
                user_batchs_by_hash.setdefault(key, [])
                user_batchs_by_hash[key].append(batch_item)
                continue
            user_id = int(float(batch_item["user_id"]))
            batch_item["user_id"] = str(user_id)

            key = user_id % len(self._mom_producers)
            user_batchs_by_hash.setdefault(key, [])
            user_batchs_by_hash[key].append(batch_item)

        for key, user_batch in user_batchs_by_hash.items():
            mom_producer = self._mom_producers[key]
            message = communication_protocol.encode_batch_message(
                message_type, session_id, user_batch
            )
            mom_producer.send(message)
