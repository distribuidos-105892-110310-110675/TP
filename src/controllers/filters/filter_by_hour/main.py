from filter import FilterByHour
from shared import initializer

FILE_PATH = 'data/transactions_202401.csv'

def main():
    config_params = initializer.init_config(["LOGGING_LEVEL", "MIN_HOUR", "MAX_HOUR"])
    initializer.init_log(config_params["LOGGING_LEVEL"])
    filter = FilterByHour(int(config_params["MIN_HOUR"]), int(config_params["MAX_HOUR"]), FILE_PATH)
    filter.start()

if __name__ == "__main__":
    main()