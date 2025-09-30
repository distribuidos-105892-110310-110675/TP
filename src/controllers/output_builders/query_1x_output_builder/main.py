import logging

from controllers.output_builders.query_1x_output_builder.query_1x_output_builder import (
    Query1XOutputBuilder,
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

    cleaner = Query1XOutputBuilder(
        controller_id=int(config_params["OUTPUT_BUILDER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumer_queue_prefix=constants.FILTERED_TRN_BY_YEAR__TIME__FINAL_AMOUNT,
        producer_queue_prefix=constants.QRS_QUEUE_PREFIX,
        previous_controllers_amount=int(config_params["PREV_CONTROLLERS_AMOUNT"]),
    )
    cleaner.run()


if __name__ == "__main__":
    main()
