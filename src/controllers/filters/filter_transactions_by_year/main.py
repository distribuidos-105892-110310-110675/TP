import logging

from controllers.filters.filter_transactions_by_year.filter_transactions_by_year import (
    FilterTransactionsByYear,
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
            "YEARS",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.debug(f"action: init_config | result: success | params: {config_params}")

    yearlist = config_params["YEARS"].split(",")
    years = [int(year) for year in yearlist]

    controller = FilterTransactionsByYear(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumer_queue_prefix=constants.CLEANED_TRN_QUEUE_PREFIX,
        producer_exchange_prefix=constants.FILTERED_TRN_BY_YEAR_EXCHANGE_PREFIX,
        producer_routing_key_prefix=constants.FILTERED_TRN_BY_YEAR_ROUTING_KEY_PREFIX,
        producer_routing_keys_amount=int(config_params["FILTERS_AMOUNT"]),
        previous_controllers_amount=int(config_params["PREV_CONTROLLERS_AMOUNT"]),
        years_to_filter=years,
    )
    controller.run()


if __name__ == "__main__":
    main()
