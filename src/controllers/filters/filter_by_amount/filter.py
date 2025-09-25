import csv
import logging
from time import sleep
import signal


class FilterByAmount:
    def __init__(self, amount, input):
        self.minimun_amount = amount
        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)
        self.running = True
        self.input = input

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, shutting down FilterByYear")
        self.running = False

    def start(self):
        logging.info("Starting FilterByYear.")
        # it should not return
        self.__get_input()
        while self.running:
            sleep(1)

    def __get_input(self):
        chunk = 1
        items = []
        i = 0
        with(open(self.input, newline='', encoding='utf-8')) as file:
            reader = csv.DictReader(file)
            for line in reader:
                items.append(line)
                if i % chunk == 0:
                    self.__filter_by_amount(items)
                    i = 0
                    items = []
            new = {'transaction_id': '1234', 'store_id': '2', 'user_id': '0', 'final_amount': '100', 'created_at': '2024-01-01 5:59:59'}
            items.append(new)
            if len(items) > 0:
                self.__filter_by_amount(items)
        file.close()

    def __filter_by_amount(self, chunk):
        for item in chunk:
            amount = float(item['final_amount'])
            if amount >= self.minimun_amount:
                self.__produce_output(item)
            else:
                logging.info(f"Transaction: {item['transaction_id']} was filtered out")
        self.running = False

    def __produce_output(self, item):
        logging.info(item)