from typing import Any

from controllers.mappers.shared.mapper import Mapper
from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_exchange import (
    RabbitMQMessageMiddlewareExchange,
)
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue


class YearHalfCreatedAtTransactonsMapper(Mapper):

    # ============================== INITIALIZE ============================== #

    def _build_mom_consumer_using(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> MessageMiddleware:
        exchange_name = consumers_config["exchange_name_prefix"]
        routing_keys = [f"{consumers_config["routing_key_prefix"]}.*"]
        return RabbitMQMessageMiddlewareExchange(
            host=rabbitmq_host,
            exchange_name=exchange_name,
            route_keys=routing_keys,
        )

    def _build_mom_producer_using(
        self,
        rabbitmq_host: str,
        producers_config: dict[str, Any],
        producer_id: int,
    ) -> MessageMiddleware:
        queue_name_prefix = producers_config["queue_name_prefix"]
        queue_name = f"{queue_name_prefix}-{producer_id}"
        return RabbitMQMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_name)

    # ============================== PRIVATE - TRANSFORM DATA ============================== #

    def _transform_batch_item(self, batch_item: dict[str, str]) -> dict[str, str]:
        date = batch_item["created_at"].split(" ")[0]
        month = date.split("-")[1]
        if int(month) <= 6:
            semester = "H1"
        else:
            semester = "H2"
        year = date.split("-")[0]
        batch_item["year_half_created_at"] = f"{year}-{semester}"
        return batch_item
