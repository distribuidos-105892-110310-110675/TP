import logging

from controllers.filters.filter_transactions_by_final_amount.filter_transactions_by_final_amount import (
    FilterTransactionsByFinalAmount,
)
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CONTROLLER_ID",
            "RABBITMQ_HOST",
            "PREV_CONTROLLERS_AMOUNT",
            "OUTPUT_BUILDERS_AMOUNT",
            "MIN_FINAL_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.debug(f"action: init_config | result: success | params: {config_params}")

    controller = FilterTransactionsByFinalAmount(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumer_exchange_prefix=constants.FILTERED_TRN_BY_YEAR__HOUR_EXCHANGE_PREFIX,
        consumer_routing_key_prefix=constants.FILTERED_TRN_BY_YEAR__HOUR_ROUTING_KEY_PREFIX,
        producer_queue_prefix=constants.FILTERED_TRN_BY_YEAR__HOUR__FINAL_AMOUNT_QUEUE_PREFIX,
        previous_controllers_amount=int(config_params["PREV_CONTROLLERS_AMOUNT"]),
        next_controllers_amount=int(config_params["OUTPUT_BUILDERS_AMOUNT"]),
        min_final_amount=float(config_params["MIN_FINAL_AMOUNT"]),
    )
    controller.run()


if __name__ == "__main__":
    main()
