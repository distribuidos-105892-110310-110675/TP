import logging

from controllers.filters.filter_items_by_year.filter_items_by_year import (
    FilterItemsByYear,
)
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CONTROLLER_ID",
            "RABBITMQ_HOST",
            "PREV_CONTROLLERS_AMOUNT",
            "MAPS_AMOUNT",
            "YEARS",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.debug(f"action: init_config | result: success | params: {config_params}")

    yearlist = config_params["YEARS"].split(",")
    years = [int(year) for year in yearlist]

    controller = FilterItemsByYear(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumer_queue_prefix=constants.CLEANED_TIT_QUEUE_PREFIX,
        producer_queue_prefix=constants.FILTERED_TIT_BY_YEAR_QUEUE_PREFIX,
        previous_controllers_amount=int(config_params["PREV_CONTROLLERS_AMOUNT"]),
        next_controllers_amount=int(config_params["MAPS_AMOUNT"]),
        years_to_filter=years,
    )
    controller.run()


if __name__ == "__main__":
    main()
