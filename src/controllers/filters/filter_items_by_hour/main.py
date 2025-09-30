from filter import FilterItemsByHour
from shared import initializer

def main():
    config_params = initializer.init_config(["LOGGING_LEVEL", "MIN_HOUR", "MAX_HOUR"])
    initializer.init_log(config_params["LOGGING_LEVEL"])
    filter = FilterItemsByHour(int(config_params["MIN_HOUR"]), int(config_params["MAX_HOUR"]))
    filter.start()

if __name__ == "__main__":
    main()