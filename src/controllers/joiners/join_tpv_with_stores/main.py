import logging

from controllers.joiners.join_tpv_with_stores.join_tpv_with_stores import (
    JoinTPVWithStores,
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
    logging.debug(f"action: init_config | result: success | params: {config_params}")

    controller = JoinTPVWithStores(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        base_data_consumer_exchange_prefix=constants.CLEANED_STR_EXCHANGE_PREFIX,
        base_data_consumer_routing_key_prefix=constants.CLEANED_STR_ROUTING_KEY_PREFIX,
        stream_consumer_queue_prefix=constants.SUM_TRN_TPV_BY_STORE_QUEUE_PREFIX,
        producer_queue_prefix=constants.TPV_BY_HALF_YEAR_CREATED_AT__STORE_NAME_QUEUE_PREFIX,
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
