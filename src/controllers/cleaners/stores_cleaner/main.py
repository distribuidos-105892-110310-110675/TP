import logging

from controllers.cleaners.stores_cleaner.stores_cleaner import StoresCleaner
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
        "queue_name_prefix": constants.DIRTY_STR_QUEUE_PREFIX,
    }
    producers_config = {
        "exchange_name_prefix": constants.CLEANED_STR_EXCHANGE_PREFIX,
        "routing_key_prefix": constants.CLEANED_STR_ROUTING_KEY_PREFIX,
        "next_controllers_amount": int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
    }

    cleaner = StoresCleaner(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumers_config=consumers_config,
        producers_config=producers_config,
    )
    cleaner.run()


if __name__ == "__main__":
    main()
