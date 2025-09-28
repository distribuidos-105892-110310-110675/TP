import logging
import signal
from shared import communication_protocol

TRANSACTIONS = [
    {"transaction_id": "t-001", "store_id": 5, 'user_id': '', 'final_amount': 24.1, "created_at": "2023-01-06 10:06:50"},
    {"transaction_id": "t-002", "store_id": 1, 'user_id': '', 'final_amount': 10.2, "created_at": "2022-01-06 10:06:50"},
    {"transaction_id": "t-003", "store_id": 2, 'user_id': '', 'final_amount': 90.1, "created_at": "2024-01-06 10:06:50"},
    {"transaction_id": "t-004", "store_id": 7, 'user_id': '', 'final_amount': 8.2, "created_at": "2024-01-06 10:06:50"},
    {"transaction_id": "t-005", "store_id": 1, 'user_id': '', 'final_amount': 30.2, "created_at": "2024-01-06 10:06:50"},
    {"transaction_id": "t-006", "store_id": 9, 'user_id': '', 'final_amount': 41.7, "created_at": "2025-01-06 10:06:50"},
    {"transaction_id": "t-007", "store_id": 1, 'user_id': '', 'final_amount': 82.9, "created_at": "2025-01-06 10:06:50"},
    {"transaction_id": "t-008", "store_id": 2, 'user_id': '', 'final_amount': 20.0, "created_at": "2025-01-06 10:06:50"},
    {"transaction_id": "t-009", "store_id": 6, 'user_id': '', 'final_amount': 77.3, "created_at": "2024-01-06 10:06:50"},
    {"transaction_id": "t-010", "store_id": 4, 'user_id': '', 'final_amount': 19.5, "created_at": "2021-01-06 10:06:50"},
]

class FilterTransactionsByAmount:
    def __init__(self, amount):
        self.minimun_amount = amount
        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)
        self.running = True
        self.processed_chunks = 0

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, shutting down FilterTransactionsByAmount.")
        self.running = False

    def start(self):
        # it should not return
        chunk_size = len(TRANSACTIONS) // 2
        i = 0
        while i < len(TRANSACTIONS):
            new_chunk = TRANSACTIONS[i:i+chunk_size]
            self.__filter_by_amount(communication_protocol.encode_transactions_batch_message(new_chunk))
            i += chunk_size
        self.__filter_by_amount(communication_protocol.encode_eof_message(communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE))

    def __filter_by_amount(self, chunk):
        msg_type = communication_protocol.decode_message_type(chunk)
        if msg_type == communication_protocol.EOF:
            logging.info(f"Received EOF message. Processed chunks: {self.processed_chunks}")
            return
        transactions = communication_protocol.decode_transactions_batch_message(chunk)
        filtered = []
        for t in transactions:
            amount = float(t['final_amount'])
            if amount >= self.minimun_amount:
                filtered.append(t)
        self.running = False
        self.__produce_output(filtered)
        self.processed_chunks += 1

    def __produce_output(self, items):
        for item in items:
            logging.info(item)