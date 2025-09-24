import csv

from filter import FilterByYear
import logging
from src.shared.initializer import init_config, init_log

FILE_PATH = '../../../../.data/transaction_items/transaction_items_202401.csv'

def main():
    # config_params = init_config(["LOGGING_LEVEL", "YEARS"])
    # init_log(config_params["LOGGING_LEVEL"])
    logging.info("Starting filter by year")
    # for now, reads from file. Should receive data and filter automatically
    filter = FilterByYear([2024, 2025])
    items = []
    with(open(FILE_PATH, newline='', encoding='utf-8')) as file:
        reader = csv.DictReader(file)
        for line in reader:
            current = {'transaction_id': line['transaction_id'], 'item_id': line['item_id'], 'quantity': line['quantity'],
                       'created_at': line['created_at']}
            items.append(current)
    file.close()
    filtered_items = filter.start(items)
    for item in filtered_items:
        print(item)
    print(f"Filtered {len(items) - len(filtered_items)} items")

if __name__ == "__main__":
    main()