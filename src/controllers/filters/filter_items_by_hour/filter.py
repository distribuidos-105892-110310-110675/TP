import logging
import signal
from shared import communication_protocol

ITEMS = [
    {"transaction_id": "ac6f851c-649f-42fb-a606-72be0fdcf8d2", "item_id": "6", "quantity": "1", "unit_price": "9.5", "subtotal": "9.5", "created_at": "2024-01-01 05:06:50"},
    {"transaction_id": "ac6f851c-649f-42fb-a606-72be0fdcf8d2", "item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-01-01 23:06:50"},
    {"transaction_id": "ac6f851c-649f-42fb-a606-72be0fdcf8d2", "item_id": "8", "quantity": "3", "unit_price": "10.0", "subtotal": "30.0", "created_at": "2024-01-01 23:00:00"},
    {"transaction_id": "d7856e66-c613-45c4-b9ca-7a3cec6c6db3", "item_id": "2", "quantity": "3", "unit_price": "7.0", "subtotal": "21.0", "created_at": "2024-01-01 06:00:00"},
    {"transaction_id": "d7856e66-c613-45c4-b9ca-7a3cec6c6db3", "item_id": "6", "quantity": "1", "unit_price": "9.5", "subtotal": "9.5", "created_at": "2024-01-01 04:06:52"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "1", "quantity": "1", "unit_price": "6.0", "subtotal": "6.0", "created_at": "2024-01-01 23:06:53"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-01-01 17:06:53"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-01-01 19:06:53"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "7", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-01-01 10:06:53"},
]


class FilterItemsByHour:
    def __init__(self, min_hour, max_hour):
        self.min_hour = min_hour
        self.max_hour = max_hour
        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)
        self.running = True
        self.processed_chunks = 0

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, shutting down FilterItemsByHour")
        self.running = False

    def start(self):
        # it should not return
        chunk_size = len(ITEMS) // 2
        i = 0
        while i < len(ITEMS):
            new_chunk = ITEMS[i:i + chunk_size]
            self.__filter_by_hour(communication_protocol.encode_transaction_items_batch_message(new_chunk))
            i += chunk_size
        self.__filter_by_hour(
            communication_protocol.encode_eof_message(communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE))

    def __filter_by_hour(self, chunk):
        msg_type = communication_protocol.decode_message_type(chunk)
        if msg_type == communication_protocol.EOF:
            logging.info(f"Received EOF message. Processed chunks: {self.processed_chunks}")
            return
        items = communication_protocol.decode_transaction_items_batch_message(chunk)
        filtered = []
        for i in items:
            hour = int(i['created_at'].split(' ')[1].split(':')[0])
            if hour >= self.min_hour and hour < self.max_hour:
                filtered.append(i)
        self.running = False
        self.processed_chunks += 1
        self.__produce_output(filtered)

    def __produce_output(self, items):
        for item in items:
            logging.info(item)