from join import JoinTransactionsWithUsers
from shared import initializer

def main():
    config_params = initializer.init_config(["LOGGING_LEVEL"])
    initializer.init_log(config_params["LOGGING_LEVEL"])
    join = JoinTransactionsWithUsers()
    join.start()

if __name__ == "__main__":
    main()