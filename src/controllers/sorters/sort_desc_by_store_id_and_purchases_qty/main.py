import logging

from controllers.sorters.sort_desc_by_store_id_and_purchases_qty.sort_desc_by_store_id_and_purchases_qty import (
    SortDescByStoreIdAndPurchasesQty,
)
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CONTROLLER_ID",
            "RABBITMQ_HOST",
            "PREV_CONTROLLERS_AMOUNT",
            "NEXT_CONTROLLERS_AMOUNT",
            "BATCH_MAX_SIZE",
            "AMOUNT_PER_GROUP",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.debug(f"action: init_config | result: success | params: {config_params}")

    controller = SortDescByStoreIdAndPurchasesQty(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumer_queue_prefix=constants.PURCHASES_QTY_BY_USR_ID__STORE_ID_QUEUE_PREFIX,
        producer_queue_prefix=constants.SORTED_DESC_BY_STORE_ID__PURCHASES_QTY_WITH_USER_ID,
        previous_controllers_amount=int(config_params["PREV_CONTROLLERS_AMOUNT"]),
        next_controllers_amount=int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
        batch_max_size=int(config_params["BATCH_MAX_SIZE"]),
        amount_per_group=int(config_params["AMOUNT_PER_GROUP"]),
    )
    controller.run()


if __name__ == "__main__":
    main()
