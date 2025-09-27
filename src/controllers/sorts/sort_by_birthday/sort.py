import signal
import logging
from time import sleep

USER_COUNT = [
    {'user_id': 1, 'purchase_amount': 20, 'store_id': 1, 'birthday': '2000-01-01'},
    {'user_id': 2, 'purchase_amount': 100, 'store_id': 2, 'birthday': '1999-01-02'},
    {'user_id': 3, 'purchase_amount': 1, 'store_id': 3, 'birthday': '1980-01-03'},
    {'user_id': 4, 'purchase_amount': 30, 'store_id': 4, 'birthday': '1982-01-04'},
    {'user_id': 5, 'purchase_amount': 19, 'store_id': 5, 'birthday': '2001-02-01'},
    {'user_id': 6, 'purchase_amount': 12, 'store_id': 1, 'birthday': '1992-02-11'},
    {'user_id': 7, 'purchase_amount': 22, 'store_id': 2, 'birthday': '1976-03-22'},
    {'user_id': 8, 'purchase_amount': 54, 'store_id': 3, 'birthday': '2004-03-31'},
    {'user_id': 9, 'purchase_amount': 45, 'store_id': 4, 'birthday': '2000-04-02'},
    {'user_id': 10, 'purchase_amount': 12, 'store_id': 5, 'birthday': '1998-04-29'},
    {'user_id': 11, 'purchase_amount': 120, 'store_id': 1, 'birthday': '2008-12-01'},
    {'user_id': 12, 'purchase_amount': 21, 'store_id': 2, 'birthday': '2003-08-25'},
    {'user_id': 13, 'purchase_amount': 5, 'store_id': 3, 'birthday': '1989-09-30'},
    {'user_id': 14, 'purchase_amount': 0, 'store_id': 4, 'birthday': '1978-10-10'},
    {'user_id': 15, 'purchase_amount': 81, 'store_id': 5, 'birthday': '1972-11-01'},
    {'user_id': 16, 'purchase_amount': 14, 'store_id': 1, 'birthday': '2007-05-28'},
    {'user_id': 17, 'purchase_amount': 11, 'store_id': 2, 'birthday': '1997-12-31'},
    {'user_id': 18, 'purchase_amount': 76, 'store_id': 3, 'birthday': '2002-07-31'},
    {'user_id': 19, 'purchase_amount': 24, 'store_id': 4, 'birthday': '2005-11-12'},
    {'user_id': 20, 'purchase_amount': 90, 'store_id': 5, 'birthday': '2006-09-19'},
    {'user_id': 21, 'purchase_amount': 0, 'store_id': 6, 'birthday': '1990-02-29'},
]

class SortByBirthday():
    def __init__(self):
        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)
        self.running = True
        self.counter = {}

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, shutting down SortByCount.")
        self.running = False

    def start(self):
        logging.info("Starting SortByCount.")
        self.__sort(USER_COUNT)
        while self.running:
            sleep(1)

    def __sort(self, chunk):
        for item in chunk:
            key = item["store_id"]

            if key not in self.counter:
                self.counter[key] = []

            self.counter[key].append({
                "user_id": item["user_id"],
                "birthday": item["birthday"],
                "purchase_amount": item["purchase_amount"],
            })

            self.counter[key] = sorted(
                self.counter[key],
                key=lambda x: x["purchase_amount"],
                reverse=True
            )[:3]

        self.__produce_output(self.counter)
        self.running = False

    def __produce_output(self, items):
        for item in items.keys():
            logging.info(f"Store id: {item}. Users: {items[item]}")
