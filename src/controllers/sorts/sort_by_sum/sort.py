import signal
import logging
from shared import communication_protocol

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
        self.received_all_items = False

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, shutting down SortBySum.")
        self.running = False

    def start(self):
        chunk_size = len(ITEM_COUNT) // 2
        i = 0
        while i < len(ITEM_COUNT):
            new_chunk = ITEM_COUNT[i:i + chunk_size]
            self.__receive_data(communication_protocol.encode_transaction_items_batch_message(new_chunk))
            i += chunk_size
        self.__receive_data(
            communication_protocol.encode_eof_message(communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE))
        self.running = False

    def __receive_data(self, chunk):
        msg_type = communication_protocol.decode_message_type(chunk)
        if msg_type == communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE:
            items = communication_protocol.decode_transaction_items_batch_message(chunk)
            self.__sort(items)
        elif msg_type == communication_protocol.EOF:
            body = communication_protocol.decode_eof_message(chunk)
            if body == communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE:
                self.received_all_items = True
                self.__produce_output(self.counter)
            else:
                logging.error("Received unknown message type")
        else:
            logging.error("Received unknown message type")

    def __sort(self, chunk):
        for item in chunk:
            key = item["year_month_created_at"]

            if (key not in self.counter or
                    item["sold"] > self.counter[key]["sold"]):
                self.counter[key] = {
                    "item_id": item['item_id'],
                    "sold": item['sold']
                }

    def __produce_output(self, items):
        for item in items.keys():
            logging.info(f"{item}: {items[item]}")
