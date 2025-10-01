import signal
import logging
from shared import communication_protocol

TRANSACTIONS = [
    {"transaction_id": "t-001", "store_id": 5, 'user_id': '', 'final_amount': 24.1, "created_at": "2024-03-06 05:06:50"},
    {"transaction_id": "t-002", "store_id": 1, 'user_id': '', 'final_amount': 10.2, "created_at": "2024-02-06 04:06:50"},
    {"transaction_id": "t-003", "store_id": 2, 'user_id': '', 'final_amount': 90.1, "created_at": "2024-01-06 10:06:50"},
    {"transaction_id": "t-004", "store_id": 7, 'user_id': '', 'final_amount': 8.2, "created_at": "2024-05-06 10:06:50"},
    {"transaction_id": "t-005", "store_id": 1, 'user_id': '', 'final_amount': 30.2, "created_at": "2024-04-06 10:06:50"},
    {"transaction_id": "t-006", "store_id": 9, 'user_id': '', 'final_amount': 41.7, "created_at": "2025-11-06 23:00:50"},
    {"transaction_id": "t-007", "store_id": 1, 'user_id': '', 'final_amount': 82.9, "created_at": "2025-010-06 10:06:50"},
    {"transaction_id": "t-008", "store_id": 2, 'user_id': '', 'final_amount': 20.0, "created_at": "2025-08-06 22:59:50"},
    {"transaction_id": "t-009", "store_id": 6, 'user_id': '', 'final_amount': 77.3, "created_at": "2025-09-06 10:06:50"},
    {"transaction_id": "t-010", "store_id": 4, 'user_id': '', 'final_amount': 19.5, "created_at": "2025-07-06 06:00:00"},
                ]

class MapMonthSemesterTransactions:
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)
        self.processed_chunks = 0

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, exiting MapMonthSemesterTransactions")

        self.running = False

    def start(self):
        chunk_size = len(TRANSACTIONS) // 2
        i = 0
        while i < len(TRANSACTIONS):
            new_chunk = TRANSACTIONS[i:i + chunk_size]
            self.__map_month_to_semester(communication_protocol.encode_transactions_batch_message(new_chunk))
            i += chunk_size
        self.__map_month_to_semester(
            communication_protocol.encode_eof_message(communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE))

    def __map_month_to_semester(self, chunk):
        msg_type = communication_protocol.decode_message_type(chunk)
        if msg_type == communication_protocol.EOF:
            logging.info(f"Received EOF message. Processed chunks: {self.processed_chunks}")
            return
        transactions = communication_protocol.decode_transactions_batch_message(chunk)
        mapped = []
        for t in transactions:
            date = t['created_at'].split(' ')[0]
            year = int(date.split('-')[0])
            month = int(date.split('-')[1])
            if month <= 6:
                semester = "H1"
            else:
                semester = "H2"
            t['year_half_created_at'] = str(year) + '-' + semester
            mapped.append(t)
        self.__produce_output(mapped)
        self.processed_chunks += 1
        self.running = False

    def __produce_output(self, items):
        for item in items:
            logging.info(item)