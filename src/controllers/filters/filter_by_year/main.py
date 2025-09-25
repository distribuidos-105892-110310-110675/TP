import csv

from filter import FilterByYear
import logging

FILE_PATH = 'data/transaction_items_202401.csv'

def main():
    # config_params = init_config(["LOGGING_LEVEL", "YEARS"])
    # init_log(config_params["LOGGING_LEVEL"])
    logging.info("Starting filter by year")
    # for now, reads from file. Should receive data and filter automatically
    filter = FilterByYear(FILE_PATH, [2024, 2025])
    filter.start()

if __name__ == "__main__":
    main()