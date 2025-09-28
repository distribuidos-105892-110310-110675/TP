import logging

from server.server import Server
from shared import constants, initializer


def __build_cleaners_data(config_params: dict) -> dict:
    menu_items_cleaners_amount = int(config_params["MENU_ITEMS_CL_AMOUNT"])
    stores_cleaners_amount = int(config_params["STORES_CL_AMOUNT"])
    transaction_items_cleaners_amount = config_params["TRANSACTION_ITEMS_CL_AMOUNT"]
    transactions_cleaners_amount = int(config_params["TRANSACTIONS_CL_AMOUNT"])
    users_cleaners_amount = int(config_params["USERS_CL_AMOUNT"])

    return {
        constants.MENU_ITEMS: {
            constants.QUEUE_PREFIX_NAME: "menu-items-cleaner-queue",
            constants.WORKERS_AMOUNT: int(menu_items_cleaners_amount),
        },
        constants.STORES: {
            constants.QUEUE_PREFIX_NAME: "stores-cleaner-queue",
            constants.WORKERS_AMOUNT: int(stores_cleaners_amount),
        },
        constants.TRANSACTION_ITEMS: {
            constants.QUEUE_PREFIX_NAME: "transaction-items-cleaner-queue",
            constants.WORKERS_AMOUNT: int(transaction_items_cleaners_amount),
        },
        constants.TRANSACTIONS: {
            constants.QUEUE_PREFIX_NAME: "transactions-cleaner-queue",
            constants.WORKERS_AMOUNT: int(transactions_cleaners_amount),
        },
        constants.USERS: {
            constants.QUEUE_PREFIX_NAME: "users-cleaner-queue",
            constants.WORKERS_AMOUNT: int(users_cleaners_amount),
        },
    }


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "SERVER_PORT",
            "SERVER_LISTEN_BACKLOG",
            "RABBITMQ_HOST",
            "MENU_ITEMS_CL_AMOUNT",
            "STORES_CL_AMOUNT",
            "TRANSACTION_ITEMS_CL_AMOUNT",
            "TRANSACTIONS_CL_AMOUNT",
            "USERS_CL_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.debug(f"action: init_config | result: success | params: {config_params}")

    server = Server(
        port=int(config_params["SERVER_PORT"]),
        listen_backlog=int(config_params["SERVER_LISTEN_BACKLOG"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        cleaners_data=__build_cleaners_data(config_params),
    )
    server.run()


if __name__ == "__main__":
    main()
