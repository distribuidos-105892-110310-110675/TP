import logging

from controllers.filters.filter_transactions_by_hour.filter_transactions_by_hour import (
    FilterTransactionsByHour,
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
            "MIN_HOUR",
            "MAX_HOUR",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    consumers_config = {
        "exchange_name_prefix": constants.FILTERED_TRN_BY_YEAR_EXCHANGE_PREFIX,
        "routing_key_prefix": constants.FILTERED_TRN_BY_YEAR_ROUTING_KEY_PREFIX,
        "prev_controllers_amount": int(config_params["PREV_CONTROLLERS_AMOUNT"]),
    }
    producers_config = {
        "exchange_name_prefix": constants.FILTERED_TRN_BY_YEAR__HOUR_EXCHANGE_PREFIX,
        "routing_key_prefix": constants.FILTERED_TRN_BY_YEAR__HOUR_ROUTING_KEY_PREFIX,
        "next_controllers_amount": int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
    }

    controller = FilterTransactionsByHour(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumers_config=consumers_config,
        producers_config=producers_config,
        min_hour=int(config_params["MIN_HOUR"]),
        max_hour=int(config_params["MAX_HOUR"]),
    )
    controller.run()


if __name__ == "__main__":
    main()
