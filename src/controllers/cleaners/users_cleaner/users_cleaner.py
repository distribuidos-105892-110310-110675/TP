import logging
from typing import Any

from controllers.cleaners.cleaner import Cleaner
from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol


class UsersCleaner(Cleaner):

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
            "user_id",
            "birthdate",
        ]

    # ============================== PRIVATE - MOM SEND/RECEIVE MESSAGES ============================== #

    def _mom_send_message_to_next(self, message: str) -> None:
        user_batchs_by_hash: dict[int, list] = {}

        for batch_item in communication_protocol.decode_users_batch_message(message):
            if batch_item["user_id"] == "":
                logging.warning(
                    f"action: invalid_user_id | user_id: {batch_item['user_id']} | result: skipped"
                )
                continue
            user_id = int(float(batch_item["user_id"]))
            batch_item["user_id"] = str(user_id)

            key = user_id % len(self._mom_producers)
            if key not in user_batchs_by_hash:
                user_batchs_by_hash[key] = []
            user_batchs_by_hash[key].append(batch_item)

        for key, user_batch in user_batchs_by_hash.items():
            mom_producer = self._mom_producers[key]
            message = communication_protocol.encode_users_batch_message(user_batch)
            mom_producer.send(message)
