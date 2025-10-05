import logging

from controllers.cleaners.users_cleaner.users_cleaner import UsersCleaner
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CONTROLLER_ID",
            "RABBITMQ_HOST",
            "JOINS_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    cleaner = UsersCleaner(
        cleaner_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        data_queue_prefix=constants.DIRTY_USR_QUEUE_PREFIX,
        cleaned_data_queue_prefix=constants.CLEANED_USR_QUEUE_PREFIX,
        cleaned_data_queues_amount=int(config_params["JOINS_AMOUNT"]),
    )
    cleaner.run()


if __name__ == "__main__":
    main()
