import logging

from controllers.reducers.count_transaction_items.count_transaction_items import (
    CountTransactionItemsByYearAndId,
)
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CONTROLLER_ID",
            "RABBITMQ_HOST",
            "PREV_CONTROLLERS_AMOUNT",
            "BATCH_MAX_SIZE",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    controller = CountTransactionItemsByYearAndId(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumer_exchange_prefix=constants.MAPPED_YEAR_MONTH_TIT_EXHCHANGE_PREFIX,
        consumer_routing_key_prefix=constants.MAPPED_YEAR_MONTH_TIT_ROUTING_KEY_PREFIX,
        producer_queue_prefix=constants.SELLINGS_QTY_BY_YEAR_MONTH_CREATED_AT__ITEM_ID_QUEUE_PREFIX,
        previous_controllers_amount=int(config_params["PREV_CONTROLLERS_AMOUNT"]),
        next_controllers_amount=1,
        batch_max_size=int(config_params["BATCH_MAX_SIZE"]),
    )

    controller.run()


if __name__ == "__main__":
    main()
