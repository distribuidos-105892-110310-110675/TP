import logging
import signal
import csv
from time import sleep


class FilterByYear:
    def __init__(self, input, years_to_filter: list[int]):

        self.years_to_filter = set(years_to_filter)

        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)
        self.running = True
        self.input = input

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, shutting down FilterByYear")
        self.running = False
        #close middleware connection

    def start(self):
        # connect to middleware
        #set callback for input
        #start consuming from middleware
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
                    self.__filter_by_year(items)
                    i = 0
                    items = []
            if len(items) > 0:
                self.__filter_by_year(items)
        file.close()

    def __filter_by_year(self, chunk):
        for item in chunk:
            year = int(item['created_at'].split(' ')[0].split('-')[0])
            if year in self.years_to_filter:
                self.__produce_output(item)
            else:
                logging.info(f"Transaction: {item['transaction_id']} was filtered out")
        self.running = False

    def __produce_output(self, item):
        logging.info(item)