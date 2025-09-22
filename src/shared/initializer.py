from configparser import ConfigParser
import logging
import os


def init_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging_level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def init_config(env_vars: list[str]) -> dict:
    """
    Get environment variables as a dictionary

    :param env_vars: list of environment variables to be collected
    :return: dictionary with the collected environment variables
    """

    try:
        config_parser = ConfigParser(os.environ)
        config_params = {}
        for env_var in env_vars:
            config_params[env_var] = os.getenv(
                env_var, config_parser["DEFAULT"][env_var]
            )
    except KeyError as e:
        raise KeyError(
            "Key was not found. Error: {}. Aborting server".format(e),
        )
    except ValueError as e:
        raise ValueError(
            "Key could not be parsed. Error: {}. Aborting server".format(e),
        )

    return config_params
