import logging

from controllers.joiners.join_item_count_with_menu.join_item_count_with_menu import (
    JoinItemCountWithMenu,
)
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CONTROLLER_ID",
            "RABBITMQ_HOST",
            "OUTPUT_BUILDERS_AMOUNT",
            "PREV_BASE_DATA_CONTROLLERS_AMOUNT",
            "PREV_STREAM_DATA_CONTROLLERS_AMOUNT",
            "NEXT_CONTROLLERS_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    controller = JoinItemCountWithMenu(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        base_data_consumer_exchange_prefix=constants.CLEANED_MIT_EXCHANGE_PREFIX,
        base_data_consumer_routing_key_prefix=constants.CLEANED_MIT_ROUTING_KEY_PREFIX,
        stream_consumer_queue_prefix=constants.SORTED_DESC_SELLINGS_QTY_BY_YEAR_MONTH__ITEM_ID_QUEUE_PREFIX,
        producer_queue_prefix=constants.SORTED_DESC_SELLINGS_QTY_BY_YEAR_MONTH__ITEM_NAME_QUEUE_PREFIX,
        previos_base_data_controllers_amount=int(
            config_params["PREV_BASE_DATA_CONTROLLERS_AMOUNT"]
        ),
        previos_stream_data_controllers_amount=int(
            config_params["PREV_STREAM_DATA_CONTROLLERS_AMOUNT"]
        ),
        next_controllers_amount=int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
    )

    controller.run()


if __name__ == "__main__":
    main()
