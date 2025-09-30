import logging

from controllers.output_builders.query_22_output_builder.query_22_output_builder import (
    Query22OutputBuilder,
)
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "OUTPUT_BUILDER_ID",
            "RABBITMQ_HOST",
            "PREV_CONTROLLERS_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.debug(f"action: init_config | result: success | params: {config_params}")

    cleaner = Query22OutputBuilder(
        controller_id=int(config_params["OUTPUT_BUILDER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumer_queue_prefix=constants.SORTED_DESC_PROFIT_SUM_BY_YEAR_MONTH__ITEM_NAME,
        producer_queue_prefix=constants.QRS_QUEUE_PREFIX,
        previous_controllers_amount=int(config_params["PREV_CONTROLLERS_AMOUNT"]),
    )
    cleaner.run()


if __name__ == "__main__":
    main()
