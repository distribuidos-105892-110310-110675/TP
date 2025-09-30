import signal
import logging
from shared import communication_protocol

TRANSACTIONS = [
    {"transaction_id": "t-001", "store_id": 5, 'user_id': '17585', 'final_amount': 24.1, "created_at": "2023-01-06 10:06:50"},
    {"transaction_id": "t-002", "store_id": 1, 'user_id': '17586', 'final_amount': 10.2, "created_at": "2022-01-06 10:06:50"},
    {"transaction_id": "t-003", "store_id": 2, 'user_id': '17586', 'final_amount': 90.1, "created_at": "2024-01-06 10:06:50"},
    {"transaction_id": "t-004", "store_id": 7, 'user_id': '17588', 'final_amount': 8.2, "created_at": "2024-01-06 10:06:50"},
    {"transaction_id": "t-005", "store_id": 1, 'user_id': '17591', 'final_amount': 30.2, "created_at": "2024-01-06 10:06:50"},
    {"transaction_id": "t-006", "store_id": 9, 'user_id': '17593', 'final_amount': 41.7, "created_at": "2025-01-06 10:06:50"},
    {"transaction_id": "t-007", "store_id": 1, 'user_id': '17593', 'final_amount': 82.9, "created_at": "2025-01-06 10:06:50"},
    {"transaction_id": "t-008", "store_id": 2, 'user_id': '17593', 'final_amount': 20.0, "created_at": "2025-01-06 10:06:50"},
    {"transaction_id": "t-009", "store_id": 6, 'user_id': '17588', 'final_amount': 77.3, "created_at": "2024-01-06 10:06:50"},
    {"transaction_id": "t-010", "store_id": 4, 'user_id': '17594', 'final_amount': 19.5, "created_at": "2021-01-06 10:06:50"},
]

USERS = [
    {"user_id": "17585", "gender": "female", "birthdate": "2003-01-06", "registered_at": "2024-01-02 10:42:29"},
    {"user_id": "17586", "gender": "female", "birthdate": "2002-03-29", "registered_at": "2024-01-02 10:42:50"},
    {"user_id": "17587", "gender": "male",   "birthdate": "1979-05-28", "registered_at": "2024-01-02 10:42:55"},
    {"user_id": "17588", "gender": "female", "birthdate": "1979-09-14", "registered_at": "2024-01-02 10:43:37"},
    {"user_id": "17589", "gender": "female", "birthdate": "1981-03-22", "registered_at": "2024-01-02 10:46:17"},
    {"user_id": "17590", "gender": "male",   "birthdate": "1986-07-20", "registered_at": "2024-01-02 10:46:41"},
    {"user_id": "17591", "gender": "female", "birthdate": "1997-05-20", "registered_at": "2024-01-02 10:48:52"},
    {"user_id": "17592", "gender": "male",   "birthdate": "2008-07-05", "registered_at": "2024-01-02 10:50:38"},
    {"user_id": "17593", "gender": "female", "birthdate": "1986-02-20", "registered_at": "2024-01-02 10:54:15"},
    {"user_id": "17594", "gender": "female", "birthdate": "2005-06-23", "registered_at": "2024-01-02 10:59:33"},
]


class JoinTransactionsWithUsers():
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)
        self.transactions = []
        self.users = []
        self.received_all_transactions = False
        self.received_all_users = False

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, shutting down JoinTransactionsWithUsers")
        self.running = False

    def start(self):
        chunk_size = len(TRANSACTIONS) // 2
        i = 0
        while i < len(TRANSACTIONS):
            new_chunk = TRANSACTIONS[i:i + chunk_size]
            self.__receive_data(communication_protocol.encode_transactions_batch_message(new_chunk))
            i += chunk_size
        self.__receive_data(
            communication_protocol.encode_eof_message(communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE))
        chunk_size = len(USERS) // 2
        i = 0
        while i < len(USERS):
            new_chunk = USERS[i:i + chunk_size]
            self.__receive_data(communication_protocol.encode_users_batch_message(new_chunk))
            i += chunk_size
        self.__receive_data(communication_protocol.encode_eof_message(communication_protocol.USERS_BATCH_MSG_TYPE))
        self.__join()

    def __receive_data(self, chunk):
        msg_type = communication_protocol.decode_message_type(chunk)
        if msg_type == communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE:
            transactions = communication_protocol.decode_transactions_batch_message(chunk)
            for t in transactions:
                self.transactions.append(t)
        elif msg_type == communication_protocol.USERS_BATCH_MSG_TYPE:
            users = communication_protocol.decode_users_batch_message(chunk)
            for u in users:
                self.users.append(u)
        elif msg_type == communication_protocol.EOF:
            body = communication_protocol.decode_eof_message(chunk)
            if body == communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE:
                self.received_all_transactions = True
            elif body == communication_protocol.USERS_BATCH_MSG_TYPE:
                self.received_all_users = True
            else:
                logging.error("Received unknown message type")
        else:
            logging.error("Received unknown message type")

    def __join(self):
        joined_items = []
        for t in self.transactions:
            for u in self.users:
                if t.get('user_id') == u.get('user_id'):
                    joined_items.append({**t, **u})
        self.running = False
        self.__produce_output(joined_items)

    def __produce_output(self, joined_items):
        for item in joined_items:
            logging.info(f"{item['transaction_id']}: {item['birthdate']}. Store: {item['store_id']}")