import logging

from server.server import Server
from shared import constants, initializer


def __build_cleaners_data(config_params: dict) -> dict:
    menu_items_workers_amount = int(config_params["MENU_ITEMS_CLN_AMOUNT"])
    stores_workers_amount = int(config_params["STORES_CLN_AMOUNT"])
    transaction_items_workers_amount = config_params["TRANSACTION_ITEMS_CLN_AMOUNT"]
    transactions_workers_amount = int(config_params["TRANSACTIONS_CLN_AMOUNT"])
    users_workers_amount = int(config_params["USERS_CLN_AMOUNT"])

    return {
        constants.MENU_ITEMS: {
            constants.QUEUE_PREFIX_NAME: "menu-items-cleaner-queue",
            constants.WORKERS_AMOUNT: int(menu_items_workers_amount),
        },
        constants.STORES: {
            constants.QUEUE_PREFIX_NAME: "stores-cleaner-queue",
            constants.WORKERS_AMOUNT: int(stores_workers_amount),
        },
        constants.TRANSACTION_ITEMS: {
            constants.QUEUE_PREFIX_NAME: "transaction-items-cleaner-queue",
            constants.WORKERS_AMOUNT: int(transaction_items_workers_amount),
        },
        constants.TRANSACTIONS: {
            constants.QUEUE_PREFIX_NAME: "transactions-cleaner-queue",
            constants.WORKERS_AMOUNT: int(transactions_workers_amount),
        },
        constants.USERS: {
            constants.QUEUE_PREFIX_NAME: "users-cleaner-queue",
            constants.WORKERS_AMOUNT: int(users_workers_amount),
        },
    }


def __build_output_builders_data(config_params: dict) -> dict:
    # @TODO: see if we want to configure workers amount for output builders too
    query_1_workers_amount = 1
    query_2_1_workers_amount = 1
    query_2_2_workers_amount = 1
    query_3_workers_amount = 1
    query_4_workers_amount = 1

    return {
        constants.QUERY_RESULT_1: {
            constants.QUEUE_PREFIX_NAME: "query-results-queue",
            constants.WORKERS_AMOUNT: int(query_1_workers_amount),
        },
        constants.QUERY_RESULT_2_1: {
            constants.QUEUE_PREFIX_NAME: "query-results-queue",
            constants.WORKERS_AMOUNT: int(query_2_1_workers_amount),
        },
        constants.QUERY_RESULT_2_2: {
            constants.QUEUE_PREFIX_NAME: "query-results-queue",
            constants.WORKERS_AMOUNT: int(query_2_2_workers_amount),
        },
        constants.QUERY_RESULT_3: {
            constants.QUEUE_PREFIX_NAME: "query-results-queue",
            constants.WORKERS_AMOUNT: int(query_3_workers_amount),
        },
        constants.QUERY_RESULT_4: {
            constants.QUEUE_PREFIX_NAME: "query-results-queue",
            constants.WORKERS_AMOUNT: int(query_4_workers_amount),
        },
    }


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "SERVER_PORT",
            "SERVER_LISTEN_BACKLOG",
            "RABBITMQ_HOST",
            "MENU_ITEMS_CLN_AMOUNT",
            "STORES_CLN_AMOUNT",
            "TRANSACTION_ITEMS_CLN_AMOUNT",
            "TRANSACTIONS_CLN_AMOUNT",
            "USERS_CLN_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.debug(f"action: init_config | result: success | params: {config_params}")

    server = Server(
        port=int(config_params["SERVER_PORT"]),
        listen_backlog=int(config_params["SERVER_LISTEN_BACKLOG"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        cleaners_data=__build_cleaners_data(config_params),
        output_builders_data=__build_output_builders_data(config_params),
    )
    server.run()


if __name__ == "__main__":
    main()
