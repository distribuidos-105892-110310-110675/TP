import logging

from server.server import Server
from shared import initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "SERVER_PORT",
            "SERVER_LISTEN_BACKLOG",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.debug(f"action: init_config | result: success | params: {config_params}")

    server = Server(
        port=int(config_params["SERVER_PORT"]),
        listen_backlog=int(config_params["SERVER_LISTEN_BACKLOG"]),
    )
    server.run()


if __name__ == "__main__":
    main()
