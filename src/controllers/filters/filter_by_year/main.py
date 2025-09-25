from shared import initializer
from filter import FilterByYear

FILE_PATH = 'data/transaction_items_202401.csv'

def main():
    config_params = initializer.init_config(["LOGGING_LEVEL", "YEARS"])
    initializer.init_log(config_params["LOGGING_LEVEL"])
    yearlist = config_params["YEARS"].split(',')
    years = []
    for year in yearlist:
        years.append(int(year))
    # for now, reads from file. Should receive data and filter automatically
    filter = FilterByYear(FILE_PATH, years)
    filter.start()

if __name__ == "__main__":
    main()