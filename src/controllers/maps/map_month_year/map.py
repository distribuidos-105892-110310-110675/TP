import signal
import logging
from time import sleep

TRANSACTION_ITEMS = [
    {"item_id": "6", "quantity": "1", "unit_price": "9.5", "subtotal": "9.5", "created_at": "2024-01-01 10:06:50"},
    {"item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-02-01 10:06:50"},
    {"item_id": "8", "quantity": "3", "unit_price": "10.0", "subtotal": "30.0", "created_at": "2024-03-01 10:06:50"},
    {"item_id": "2", "quantity": "3", "unit_price": "7.0", "subtotal": "21.0", "created_at": "2024-04-01 10:06:52"},
    {"item_id": "6", "quantity": "1", "unit_price": "9.5", "subtotal": "9.5", "created_at": "2025-01-01 10:06:52"},
    {"item_id": "1", "quantity": "1", "unit_price": "6.0", "subtotal": "6.0", "created_at": "2025-02-01 10:06:53"},
    {"item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2025-03-01 10:06:53"},
    {"item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2025-04-01 10:06:53"},
]

class MapMonthYear:
    def __init__(self):
        self.running = True

        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, exiting MapYearMonth")
        self.running = False

    def start(self):
        logging.info("Starting MapYearMonth")
        self.__map_year_to_month(TRANSACTION_ITEMS)
        while self.running:
            sleep(1)

    def __map_year_to_month(self, items):
        mapped = []
        for item in items:
            date = item['created_at'].split(' ')[0]
            month = (date.split('-')[1])
            year = (date.split('-')[0])
            item['year_month_created_at'] = year + '-' + month
            mapped.append(item)
        self.__produce_output(mapped)
        self.running = False

    def __produce_output(self, items):
        logging.info("MapYearMonth producing output")
        for item in items:
            logging.info(item)