import logging

from controllers.output_builders.query_21_output_builder.query_21_output_builder import (
    Query21OutputBuilder,
)
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CONTROLLER_ID",
            "RABBITMQ_HOST",
            "PREV_CONTROLLERS_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    consumers_config = {
        "queue_name_prefix": constants.SORTED_DESC_SELLINGS_QTY_BY_YEAR_MONTH__ITEM_NAME_QUEUE_PREFIX,
    }
    producers_config = {
        "queue_name_prefix": constants.QRS_QUEUE_PREFIX,
    }

    cleaner = Query21OutputBuilder(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumers_config=consumers_config,
        producers_config=producers_config,
        prev_controllers_amount=int(config_params["PREV_CONTROLLERS_AMOUNT"]),
    )
    cleaner.run()


if __name__ == "__main__":
    main()
