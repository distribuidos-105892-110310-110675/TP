import csv
import logging
from time import sleep
import signal


class FilterByHour:
    def __init__(self, min_hour, max_hour, input):
        self.min_hour = min_hour
        self.max_hour = max_hour
        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)
        self.running = True
        self.input = input

    def __handle_sigterm_signal(self, signal, frame):
        print("Caught SIGTERM. Exiting.")
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
                current = {'transaction_id': line['transaction_id'], 'store_id': line['store_id'], 'user_id': line['user_id'],
                           'final_amount': line['final_amount'], 'created_at': line['created_at']}
                items.append(current)
                if i % chunk == 0:
                    self.__filter_by_hour(items)
                    i = 0
                    items = []
            new = {'transaction_id': '1234', 'store_id': '2', 'user_id': '0', 'final_amount': '100', 'created_at': '2024-01-01 5:59:59'}
            new2 = {'transaction_id': '1235', 'store_id': '2', 'user_id': '0', 'final_amount': '100', 'created_at': '2024-01-01 23:00:01'}
            new3 = {'transaction_id': '1236', 'store_id': '2', 'user_id': '0', 'final_amount': '100', 'created_at': '2024-01-01 22:59:59'}
            items.append(new)
            items.append(new2)
            items.append(new3)
            if len(items) > 0:
                self.__filter_by_hour(items)
        file.close()

    def __filter_by_hour(self, chunk):
        for item in chunk:
            hour = int(item['created_at'].split(' ')[1].split(':')[0])
            if hour >= self.min_hour and hour < self.max_hour:
                self.__produce_output(item)
            else:
                print(f"Transaction: {item['transaction_id']} was filtered out")
                logging.info(f"Transaction: {item['transaction_id']} was filtered out")
        self.running = False

    def __produce_output(self, item):
        print(item)