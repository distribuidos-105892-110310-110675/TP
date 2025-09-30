import logging

from controllers.cleaners.stores_cleaner.stores_cleaner import StoresCleaner
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CLEANER_ID",
            "RABBITMQ_HOST",
            "JOINS_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.debug(f"action: init_config | result: success | params: {config_params}")

    cleaner = StoresCleaner(
        cleaner_id=int(config_params["CLEANER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        data_queue_prefix=constants.DIRTY_STR_QUEUE_PREFIX,
        cleaned_data_exchange_prefix=constants.CLEANED_STR_EXCHANGE_PREFIX,
        cleaned_data_routing_key_prefix=constants.CLEANED_STR_ROUTING_KEY_PREFIX,
        cleaned_data_routing_keys_amount=int(config_params["JOINS_AMOUNT"]),
    )
    cleaner.run()


if __name__ == "__main__":
    main()
