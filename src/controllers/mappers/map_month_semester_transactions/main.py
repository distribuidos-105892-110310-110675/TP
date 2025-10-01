import logging

from controllers.mappers.map_month_semester_transactions.map_month_semester_transactions import (
    MapMonthSemesterTransactions,
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

    controller = MapMonthSemesterTransactions(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumer_exchange_prefix=constants.FILTERED_TRN_BY_YEAR__HOUR_EXCHANGE_PREFIX,
        consumer_routing_key_prefix=constants.FILTERED_TRN_BY_YEAR__HOUR_ROUTING_KEY_PREFIX,
        producer_queue_prefix=constants.MAPPED_TRN_SEMESTER_QUEUE_PREFIX,
        next_controllers_amount=int(config_params["REDUCERS_AMOUNT"]),
        previous_controllers_amount=int(config_params["PREV_CONTROLLERS_AMOUNT"])
    )
    controller.run()


if __name__ == "__main__":
    main()