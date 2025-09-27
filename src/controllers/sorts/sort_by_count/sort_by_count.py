import signal
import logging
from time import sleep

ITEM_COUNT = [
    {'item_id': 1, 'quantity': 20, 'year_month_created_at': '2024-01'},
    {'item_id': 2, 'quantity': 100, 'year_month_created_at': '2024-02'},
    {'item_id': 3, 'quantity': 1, 'year_month_created_at': '2024-03'},
    {'item_id': 4, 'quantity': 30, 'year_month_created_at': '2024-01'},
    {'item_id': 5, 'quantity': 19, 'year_month_created_at': '2024-02'},
    {'item_id': 6, 'quantity': 12, 'year_month_created_at': '2024-03'},
    {'item_id': 7, 'quantity': 22, 'year_month_created_at': '2024-01'},
    {'item_id': 8, 'quantity': 54, 'year_month_created_at': '2024-02'},
    {'item_id': 9, 'quantity': 45, 'year_month_created_at': '2024-03'},
    {'item_id': 10, 'quantity': 12, 'year_month_created_at': '2025-01'},
    {'item_id': 1, 'quantity': 120, 'year_month_created_at': '2025-02'},
    {'item_id': 2, 'quantity': 21, 'year_month_created_at': '2025-03'},
    {'item_id': 3, 'quantity': 5, 'year_month_created_at': '2025-01'},
    {'item_id': 4, 'quantity': 0, 'year_month_created_at': '2025-02'},
    {'item_id': 5, 'quantity': 81, 'year_month_created_at': '2025-03'},
    {'item_id': 6, 'quantity': 14, 'year_month_created_at': '2025-01'},
    {'item_id': 7, 'quantity': 11, 'year_month_created_at': '2025-02'},
    {'item_id': 8, 'quantity': 76, 'year_month_created_at': '2025-03'},
    {'item_id': 9, 'quantity': 24, 'year_month_created_at': '2025-01'},
    {'item_id': 10, 'quantity': 90, 'year_month_created_at': '2024-02'},
    {'item_id': 1, 'quantity': 0, 'year_month_created_at': '2025-04'},
]

class SortByCount():
    def __init__(self):
        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)
        self.running = True
        self.counter = {}

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, shutting down SortByCount.")
        self.running = False

    def start(self):
        logging.info("Starting SortByCount.")
        self.__sort(ITEM_COUNT)
        while self.running:
            sleep(1)

    def __sort(self, chunk):
        for item in chunk:
            key = item["year_month_created_at"]

            if (key not in self.counter or
                    item["quantity"] > self.counter[key]["quantity"]):
                self.counter[key] = {
                    "item_id": item['item_id'],
                    "quantity": item['quantity']
                }

        self.__produce_output(self.counter)
        self.running = False

    def __produce_output(self, items):
        for item in items.keys():
            print(item, items[item])
