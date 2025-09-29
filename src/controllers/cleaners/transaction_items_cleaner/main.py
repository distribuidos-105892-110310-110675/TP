import logging

from controllers.cleaners.transaction_items_cleaner.transaction_items_cleaner import (
    TransactionItemsCleaner,
)
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CLEANER_ID",
            "RABBITMQ_HOST",
            "FILTERS_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.debug(f"action: init_config | result: success | params: {config_params}")

    cleaner = TransactionItemsCleaner(
        cleaner_id=int(config_params["CLEANER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        cleaner_queue_prefix=constants.TIT_CLEANER_QUEUE_PREFIX,
        filters_queue_prefix=constants.FILTER_TIT_BY_YEAR_QUEUE_PREFIX,
        filters_amount=int(config_params["FILTERS_AMOUNT"]),
    )
    cleaner.run()


if __name__ == "__main__":
    main()
