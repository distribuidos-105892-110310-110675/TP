import logging

from controllers.joins.join_item_sum_with_menu.join_item_sum_with_menu import (
    JoinItemSumWithMenu,
)
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CONTROLLER_ID",
            "RABBITMQ_HOST",
            "OUTPUT_BUILDERS_AMOUNT",
            "PREVIOUS_CONTROLLERS_AMOUNT",
            "PREVIOUS_MENU_ITEMS_SENDERS",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.debug(f"action: init_config | result: success | params: {config_params}")

    controller = JoinItemSumWithMenu(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumer_queue_prefix=constants.SORTED_DESC_PROFIT_SUM_BY_YEAR_MONTH__ITEM_NAME_QUEUE_PREFIX,
        producer_queue_prefix=constants.SORTED_DESC_PROFIT_SUM_BY_YEAR_MONTH__ITEM_ID_QUEUE_PREFIX,
        consumer_exchange_prefix=constants.CLEANED_MIT_EXCHANGE_PREFIX,
        consumer_routing_key_prefix=constants.CLEANED_MIT_ROUTING_KEY_PREFIX,
        producer_queue_amount=int(config_params["OUTPUT_BUILDERS_AMOUNT"]),
        previous_controller_amount=int(config_params["PREVIOUS_CONTROLLERS_AMOUNT"]),
        previous_menu_items_senders=int(config_params["PREVIOUS_MENU_ITEMS_SENDERS"]),
    )

    controller.run()


if __name__ == "__main__":
    main()
