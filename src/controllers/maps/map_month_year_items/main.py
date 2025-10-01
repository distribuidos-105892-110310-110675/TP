import logging

from controllers.maps.map_month_year_items.map_month_year_items import (
    MapMonthYearItems,
)
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CONTROLLER_ID",
            "RABBITMQ_HOST",
            "PREV_CONTROLLERS_AMOUNT",
            "REDUCERS_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.debug(f"action: init_config | result: success | params: {config_params}")

    controller = MapMonthYearItems(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumer_queue_prefix=constants.FILTERED_TIT_QUEUE_PREFIX,
        producer_exchange_prefix=constants.MAPPED_YEAR_MONTH_TIT_EXHCHANGE_PREFIX,
        producer_routing_key_prefix=constants.MAPPED_YEAR_MONTH_TIT_ROUTING_KEY_PREFIX,
        producer_routing_keys_amount=int(config_params["REDUCERS_AMOUNT"]),
        previous_controllers_amount=int(config_params["PREV_CONTROLLERS_AMOUNT"]),
    )
    controller.run()


if __name__ == "__main__":
    main()