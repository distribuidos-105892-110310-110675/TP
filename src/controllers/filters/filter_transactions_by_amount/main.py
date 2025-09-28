from filter import FilterTransactionsByAmount
from shared import initializer

def main():
    config_params = initializer.init_config(["LOGGING_LEVEL", "AMOUNT"])
    initializer.init_log(config_params["LOGGING_LEVEL"])
    filter = FilterTransactionsByAmount(int(config_params["AMOUNT"]))
    filter.start()

if __name__ == "__main__":
    main()