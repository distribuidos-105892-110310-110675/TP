from typing import Any

from controllers.reducers.shared.reducer import Reducer
from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_exchange import (
    RabbitMQMessageMiddlewareExchange,
)
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol


class PurchasesQtyByStoreIdAndUserIdReducer(Reducer):

    # ============================== INITIALIZE ============================== #

    def _build_mom_consumer_using(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> MessageMiddleware:
        exchange_name = consumers_config["exchange_name_prefix"]
        routing_key = f"{consumers_config["routing_key_prefix"]}.{self._controller_id}"
        return RabbitMQMessageMiddlewareExchange(
            host=rabbitmq_host,
            exchange_name=exchange_name,
            route_keys=[routing_key],
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

    # ============================== PRIVATE - ACCESSING ============================== #

    def _keys(self) -> list[str]:
        return ["store_id", "user_id"]

    def _accumulator_name(self) -> str:
        return "purchases_qty"

    def _message_type(self) -> str:
        return communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE

    # ============================== PRIVATE - HANDLE DATA ============================== #

    def _reduce_function(
        self, current_value: float, batch_item: dict[str, str]
    ) -> float:
        return current_value + 1

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
