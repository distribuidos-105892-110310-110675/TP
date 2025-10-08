import logging

from controllers.cleaners.transaction_items_cleaner.transaction_items_cleaner import (
    TransactionItemsCleaner,
)
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CONTROLLER_ID",
            "RABBITMQ_HOST",
            "NEXT_CONTROLLERS_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    consumers_config = {
        "queue_name_prefix": constants.DIRTY_TIT_QUEUE_PREFIX,
    }
    producers_config = {
        "queue_name_prefix": constants.CLEANED_TIT_QUEUE_PREFIX,
        "next_controllers_amount": int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
    }

    cleaner = TransactionItemsCleaner(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumers_config=consumers_config,
        producers_config=producers_config,
    )
    cleaner.run()


if __name__ == "__main__":
    main()
