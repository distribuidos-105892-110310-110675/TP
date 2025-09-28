import signal
import logging
from time import sleep

TRANSACTION_ITEMS = [
    {"item_id": "6", "quantity": "1", "unit_price": "9.5", "subtotal": "9.5", "created_at": "2024-01-01 10:06:50"},
    {"item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-02-01 10:06:50"},
    {"item_id": "8", "quantity": "3", "unit_price": "10.0", "subtotal": "30.0", "created_at": "2024-07-01 10:06:50"},
    {"item_id": "2", "quantity": "3", "unit_price": "7.0", "subtotal": "21.0", "created_at": "2024-08-01 10:06:52"},
    {"item_id": "6", "quantity": "1", "unit_price": "9.5", "subtotal": "9.5", "created_at": "2025-01-01 10:06:52"},
    {"item_id": "1", "quantity": "1", "unit_price": "6.0", "subtotal": "6.0", "created_at": "2025-02-01 10:06:53"},
    {"item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2025-09-01 10:06:53"},
    {"item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2025-10-01 10:06:53"},
]

class MapMonthSemester:
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, exiting MapYearMonth")

        self.running = False

    def start(self):
        logging.info("Starting MapYearMonth")
        self.__map_month_to_semester(TRANSACTION_ITEMS)
        while self.running:
            sleep(1)

    def __map_month_to_semester(self, items):
        mapped = []
        for item in items:
            date = item['created_at'].split(' ')[0]
            year = int(date.split('-')[0])
            month = int(date.split('-')[1])
            if month <= 6:
                semester = "H1"
            else:
                semester = "H2"
            item['year_half_created_at'] = str(year) + '-' + semester
            mapped.append(item)
        self.__produce_output(mapped)
        self.running = False

    def __produce_output(self, items):
        logging.info("MapMonthSemester producing output")
        for item in items:
            logging.info(item)