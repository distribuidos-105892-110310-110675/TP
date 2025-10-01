import logging

from controllers.reducers.count_purchases_by_store_id_and_user_id.count_purchases_by_store_id_and_user_id import (
    CountPurchasesByStoreIdAndUserId,
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
    logging.debug(f"action: init_config | result: success | params: {config_params}")

    controller = CountPurchasesByStoreIdAndUserId(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumer_exchange_prefix=constants.FILTERED_TRN_BY_YEAR_EXCHANGE_PREFIX,
        consumer_routing_key_prefix=constants.FILTERED_TRN_BY_YEAR_ROUTING_KEY_PREFIX,
        producer_queue_prefix=constants.PURCHASES_QTY_BY_USR_ID__STORE_ID_QUEUE_PREFIX,
        previous_controllers_amount= int(config_params["PREV_CONTROLLERS_AMOUNT"]),
        next_controllers_amount=1,
        batch_max_size=int(config_params["BATCH_MAX_SIZE"]),
    )
    controller.run()


if __name__ == "__main__":
    main()
