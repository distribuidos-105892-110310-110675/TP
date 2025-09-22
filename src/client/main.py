import logging
from client.client import Client
from shared import initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CLIENT_ID",
            "SERVER_HOST",
            "SERVER_PORT",
            "DATA_PATH",
            "BATCH_MAX_KIB",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.debug(f"action: init_config | result: success | params: {config_params}")

    client = Client(
        client_id=int(config_params["CLIENT_ID"]),
        server_host=config_params["SERVER_HOST"],
        server_port=int(config_params["SERVER_PORT"]),
        data_path=config_params["DATA_PATH"],
        batch_max_kib=int(config_params["BATCH_MAX_KIB"]),
    )
    client.run()


if __name__ == "__main__":
    main()
