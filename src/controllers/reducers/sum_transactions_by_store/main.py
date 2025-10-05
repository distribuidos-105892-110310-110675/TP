import logging

from controllers.reducers.sum_transactions_by_store.sum_transactions_by_store import (
    SumTransactionsByStore,
)
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CONTROLLER_ID",
            "RABBITMQ_HOST",
            "PREV_CONTROLLERS_AMOUNT",
            "BATCH_MAX_SIZE",
            "JOINS_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    controller = SumTransactionsByStore(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumer_queue_prefix=constants.MAPPED_TRN_SEMESTER_QUEUE_PREFIX,
        previous_controllers_amount=int(config_params["PREV_CONTROLLERS_AMOUNT"]),
        producer_queue_prefix=constants.SUM_TRN_TPV_BY_STORE_QUEUE_PREFIX,
        next_controllers_amount=int(config_params["JOINS_AMOUNT"]),
        batch_max_size=int(config_params["BATCH_MAX_SIZE"]),
    )

    controller.run()


if __name__ == "__main__":
    main()
