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
            "FILTERS_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    cleaner = TransactionItemsCleaner(
        cleaner_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        data_queue_prefix=constants.DIRTY_TIT_QUEUE_PREFIX,
        cleaned_data_queue_prefix=constants.CLEANED_TIT_QUEUE_PREFIX,
        cleaned_data_queues_amount=int(config_params["FILTERS_AMOUNT"]),
    )
    cleaner.run()


if __name__ == "__main__":
    main()
