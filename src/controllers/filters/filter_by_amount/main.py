from filter import FilterByAmount
from shared import initializer

FILE_PATH = 'data/transactions_202401.csv'

def main():
    config_params = initializer.init_config(["LOGGING_LEVEL", "AMOUNT"])
    initializer.init_log(config_params["LOGGING_LEVEL"])
    filter = FilterByAmount(int(config_params["AMOUNT"]), FILE_PATH)
    filter.start()

if __name__ == "__main__":
    main()