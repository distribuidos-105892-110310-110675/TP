import logging

from controllers.joiners.join_transactions_with_users.join_transactions_with_users import (
    JoinTransactionsWithUsers,
)
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CONTROLLER_ID",
            "RABBITMQ_HOST",
            "PREV_BASE_DATA_CONTROLLERS_AMOUNT",
            "PREV_STREAM_DATA_CONTROLLERS_AMOUNT",
            "NEXT_CONTROLLERS_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    controller = JoinTransactionsWithUsers(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        base_data_consumer_queue_prefix=constants.CLEANED_USR_QUEUE_PREFIX,
        stream_consumer_queue_prefix=constants.SORTED_DESC_BY_STORE_ID__PURCHASES_QTY_WITH_USER_ID,
        producer_queue_prefix=constants.SORTED_DESC_BY_STORE_ID__PURCHASES_QTY_WITH_USER_BITHDATE,
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
