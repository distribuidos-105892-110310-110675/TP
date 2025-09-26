import signal
import logging
from time import sleep
import csv
import calendar

class MapMonthYear:
    def __init__(self, input):
        self.input = input
        self.running = True

        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, exiting MapYearMonth")
        self.running = False

    def start(self):
        logging.info("Starting MapYearMonth")
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
                    self.__map_year_to_month(items)
                    i = 0
                    items = []
            new = {'transaction_id': '1234', 'store_id': '2', 'user_id': '0', 'final_amount': '100',
                   'created_at': '2024-11-01 5:59:59'}
            new2 = {'transaction_id': '1235', 'store_id': '2', 'user_id': '0', 'final_amount': '100',
                    'created_at': '2024-08-01 23:00:01'}
            new3 = {'transaction_id': '1236', 'store_id': '2', 'user_id': '0', 'final_amount': '100',
                    'created_at': '2024-06-01 22:59:59'}
            items.append(new)
            items.append(new2)
            items.append(new3)
            if len(items) > 0:
                self.__map_year_to_month(items)
        file.close()
        self.running = False

    def __map_year_to_month(self, items):
        for item in items:
            date = item['created_at'].split(' ')[0]
            month = (date.split('-')[1])
            year = (date.split('-')[0])
            item['year_month_created_at'] = year + '-' + month
            self.__produce_output(item)

    def __produce_output(self, item):
        print(item)