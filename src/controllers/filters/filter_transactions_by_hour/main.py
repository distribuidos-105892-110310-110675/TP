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
            "FILTERS_AMOUNT",
            "MIN_HOUR",
            "MAX_HOUR",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    controller = FilterTransactionsByHour(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumer_exchange_prefix=constants.FILTERED_TRN_BY_YEAR_EXCHANGE_PREFIX,
        consumer_routing_key_prefix=constants.FILTERED_TRN_BY_YEAR_ROUTING_KEY_PREFIX,
        producer_exchange_prefix=constants.FILTERED_TRN_BY_YEAR__HOUR_EXCHANGE_PREFIX,
        producer_routing_key_prefix=constants.FILTERED_TRN_BY_YEAR__HOUR_ROUTING_KEY_PREFIX,
        producer_routing_keys_amount=int(config_params["FILTERS_AMOUNT"]),
        previous_controllers_amount=int(config_params["PREV_CONTROLLERS_AMOUNT"]),
        min_hour=int(config_params["MIN_HOUR"]),
        max_hour=int(config_params["MAX_HOUR"]),
    )
    controller.run()


if __name__ == "__main__":
    main()
