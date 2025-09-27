import signal
import logging
from time import sleep

ITEM_COUNT = [
    {'item_id': 1, 'sold': 19002, 'year_month_created_at': '2024-01'},
    {'item_id': 2, 'sold': 10101, 'year_month_created_at': '2024-02'},
    {'item_id': 3, 'sold': 20120, 'year_month_created_at': '2024-03'},
    {'item_id': 4, 'sold': 20102, 'year_month_created_at': '2024-01'},
    {'item_id': 5, 'sold': 24920, 'year_month_created_at': '2024-02'},
    {'item_id': 6, 'sold': 49180, 'year_month_created_at': '2024-03'},
    {'item_id': 7, 'sold': 30101, 'year_month_created_at': '2024-01'},
    {'item_id': 8, 'sold': 39211, 'year_month_created_at': '2024-02'},
    {'item_id': 9, 'sold': 58101, 'year_month_created_at': '2024-03'},
    {'item_id': 10, 'sold': 38102, 'year_month_created_at': '2025-01'},
    {'item_id': 1, 'sold': 12019, 'year_month_created_at': '2025-02'},
    {'item_id': 2, 'sold': 10201, 'year_month_created_at': '2025-03'},
    {'item_id': 3, 'sold': 59103, 'year_month_created_at': '2025-01'},
    {'item_id': 4, 'sold': 20394, 'year_month_created_at': '2025-02'},
    {'item_id': 5, 'sold': 58200, 'year_month_created_at': '2025-03'},
    {'item_id': 6, 'sold': 80901, 'year_month_created_at': '2025-01'},
    {'item_id': 7, 'sold': 91902, 'year_month_created_at': '2025-02'},
    {'item_id': 8, 'sold': 100129, 'year_month_created_at': '2025-03'},
    {'item_id': 9, 'sold': 29290, 'year_month_created_at': '2025-01'},
    {'item_id': 10, 'sold': 92910, 'year_month_created_at': '2024-02'},
    {'item_id': 1, 'sold': 1, 'year_month_created_at': '2025-04'},
]

class SortBySum():
    def __init__(self):
        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)
        self.running = True
        self.counter = {}

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, shutting down SortBySum.")
        self.running = False

    def start(self):
        logging.info("Starting SortBySum.")
        self.__sort(ITEM_COUNT)
        while self.running:
            sleep(1)

    def __sort(self, chunk):
        for item in chunk:
            key = item["year_month_created_at"]

            if (key not in self.counter or
                    item["sold"] > self.counter[key]["sold"]):
                self.counter[key] = {
                    "item_id": item['item_id'],
                    "sold": item['sold']
                }

        self.__produce_output(self.counter)
        self.running = False

    def __produce_output(self, items):
        for item in items.keys():
            logging.info(f"{item}: {items[item]}")
