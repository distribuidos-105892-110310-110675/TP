import logging
import signal
from time import sleep

ITEMS = [
    {"transaction_id": "ac6f851c-649f-42fb-a606-72be0fdcf8d2", "item_id": "6", "quantity": "1", "unit_price": "9.5", "subtotal": "9.5", "created_at": "2024-01-01 10:06:50"},
    {"transaction_id": "ac6f851c-649f-42fb-a606-72be0fdcf8d2", "item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-01-01 10:06:50"},
    {"transaction_id": "ac6f851c-649f-42fb-a606-72be0fdcf8d2", "item_id": "8", "quantity": "3", "unit_price": "10.0", "subtotal": "30.0", "created_at": "2024-01-01 10:06:50"},
    {"transaction_id": "d7856e66-c613-45c4-b9ca-7a3cec6c6db3", "item_id": "2", "quantity": "3", "unit_price": "7.0", "subtotal": "21.0", "created_at": "2024-01-01 10:06:52"},
    {"transaction_id": "d7856e66-c613-45c4-b9ca-7a3cec6c6db3", "item_id": "6", "quantity": "1", "unit_price": "9.5", "subtotal": "9.5", "created_at": "2024-01-01 10:06:52"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "1", "quantity": "1", "unit_price": "6.0", "subtotal": "6.0", "created_at": "2024-01-01 10:06:53"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-01-01 10:06:53"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-01-01 10:06:53"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "7", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-01-01 10:06:53"},
]

class FilterItemsByYear:
    def __init__(self, years_to_filter: list[int]):

        self.years_to_filter = set(years_to_filter)

        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)
        self.running = True

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, shutting down FilterByYear")
        self.running = False
        #close middleware connection

    def start(self):
        # connect to middleware
        #set callback for input
        #start consuming from middleware
        # it should not return
        self.__filter_by_year(ITEMS)
        while self.running:
            sleep(1)

    def __filter_by_year(self, chunk):
        filtered = []
        for item in chunk:
            year = int(item['created_at'].split(' ')[0].split('-')[0])
            if year in self.years_to_filter:
                filtered.append(item)
        self.running = False
        self.__produce_output(filtered)

    def __produce_output(self, items):
        for item in items:
            logging.info(item)