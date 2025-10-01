from map import MapMonthSemesterTransactions
from shared import initializer

def main():
    config_params = initializer.init_config(["LOGGING_LEVEL"])
    initializer.init_log(config_params["LOGGING_LEVEL"])
    mapper = MapMonthSemesterTransactions()
    mapper.start()

if __name__ == '__main__':
    main()
