from typing import Any

from controllers.mappers.shared.mapper import Mapper
from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_exchange import (
    RabbitMQMessageMiddlewareExchange,
)
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol


class YearHalfCreatedAtTransactonsMapper(Mapper):

    # ============================== INITIALIZE ============================== #

    def _build_mom_consumer_using(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> MessageMiddleware:
        exchange_name = consumers_config["exchange_name_prefix"]
        routing_keys = [
            f"{consumers_config["routing_key_prefix"]}.{self._controller_id}"
        ]
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

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_to_next(self, message: str) -> None:
        batchs_by_hash: dict[int, list] = {}
        # [IMPORTANT] this must consider the next controller's grouping key
        sharding_key = "store_id"

        message_type = communication_protocol.get_message_type(message)
        session_id = communication_protocol.get_message_session_id(message)
        for batch_item in communication_protocol.decode_batch_message(message):
            if batch_item[sharding_key] == "":
                # [IMPORTANT] If sharding value is empty, the hash will fail
                # but we are going to assign it to the first reducer anyway
                hash = 0
                batchs_by_hash.setdefault(hash, [])
                batchs_by_hash[hash].append(batch_item)
                continue
            sharding_value = int(float(batch_item[sharding_key]))
            batch_item[sharding_key] = str(sharding_value)

            hash = sharding_value % len(self._mom_producers)
            batchs_by_hash.setdefault(hash, [])
            batchs_by_hash[hash].append(batch_item)

        for hash, user_batch in batchs_by_hash.items():
            mom_producer = self._mom_producers[hash]
            message = communication_protocol.encode_batch_message(
                message_type, session_id, user_batch
            )
            mom_producer.send(message)
