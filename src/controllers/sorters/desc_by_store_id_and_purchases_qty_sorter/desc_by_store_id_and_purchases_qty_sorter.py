import logging
from typing import Any

from controllers.sorters.shared.sorter import Sorter
from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol


class DescByStoreIdAndPurchasesQtySorter(Sorter):

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
        queue_name_prefix = producers_config["queue_name_prefix"]
        queue_name = f"{queue_name_prefix}-{producer_id}"
        return RabbitMQMessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_name)

    # ============================== PRIVATE - ACCESSING ============================== #

    def _grouping_key(self) -> str:
        return "store_id"

    def _primary_sort_key(self) -> str:
        return "store_id"

    def _secondary_sort_key(self) -> str:
        return "purchases_qty"

    def _message_type(self) -> str:
        return communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_to_next(self, message: str) -> None:
        batchs_by_hash: dict[int, list] = {}
        # [IMPORTANT] this must consider the next controller's grouping key
        sharding_key = "user_id"

        message_type = communication_protocol.get_message_type(message)
        session_id = communication_protocol.get_message_session_id(message)
        for batch_item in communication_protocol.decode_batch_message(message):
            if batch_item[sharding_key] == "":
                logging.warning(
                    f"action: invalid_{sharding_key} | {sharding_key}: {batch_item[sharding_key]} | result: skipped"
                )
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
