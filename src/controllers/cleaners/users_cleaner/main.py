import logging

from controllers.cleaners.users_cleaner.users_cleaner import UsersCleaner
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CLEANER_ID",
            "RABBITMQ_HOST",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.debug(f"action: init_config | result: success | params: {config_params}")

    cleaner = UsersCleaner(
        cleaner_id=int(config_params["CLEANER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        data_queue_prefix=constants.USR_CLEANER_QUEUE_PREFIX,
        cleaned_data_queue_prefix=constants.CLEANED_USR_QUEUE_PREFIX,
    )
    cleaner.run()


if __name__ == "__main__":
    main()
