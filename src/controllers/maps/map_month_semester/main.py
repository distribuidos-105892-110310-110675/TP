from map import MapMonthSemester
from shared import initializer

def main():
    config_params = initializer.init_config(["LOGGING_LEVEL"])
    initializer.init_log(config_params["LOGGING_LEVEL"])
    mapper = MapMonthSemester()
    mapper.start()

if __name__ == '__main__':
    main()
