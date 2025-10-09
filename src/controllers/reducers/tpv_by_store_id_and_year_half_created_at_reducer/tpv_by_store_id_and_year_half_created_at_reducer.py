from typing import Any

from controllers.reducers.reducer import Reducer
from middleware.middleware import MessageMiddleware
from middleware.rabbitmq_message_middleware_queue import RabbitMQMessageMiddlewareQueue
from shared import communication_protocol


class TpvByStoreIdAndYearHalfCreatedAtReducer(Reducer):

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

    def _keys(self) -> list[str]:
        return ["store_id", "year_half_created_at"]

    def _accumulator_name(self) -> str:
        return "tpv"

    def _message_type(self) -> str:
        return communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE

    # ============================== PRIVATE - HANDLE DATA ============================== #

    def _reduce_function(
        self, current_value: float, batch_item: dict[str, str]
    ) -> float:
        return current_value + float(batch_item["final_amount"])
