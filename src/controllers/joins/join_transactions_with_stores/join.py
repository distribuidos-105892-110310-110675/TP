import signal
import logging
from time import sleep

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

STORES = [
    {'store_id': 1, 'store_name': 'G Coffee @ USJ 89q', 'street': 'Jalan Dewan Bahasa 5/9', 'postal_code': '50998', 'city': 'USJ 89q', 'state': 'Kuala Lumpur', 'latitude': '3.117134', 'longitude': '101.615027'},
{'store_id': 2, 'store_name': 'G Coffee @ Kondominium Putra', 'street': 'Jln Yew 6X', 'postal_code': '63826', 'city': 'Kondominium Putra', 'state': 'Selangor Darul Ehsan', 'latitude': '2.959571', 'longitude': '101.51772'},
{'store_id': 3, 'store_name': 'G Coffee @ USJ 57W', 'street': 'Jalan Bukit Petaling 5/16C', 'postal_code': '62094', 'city': 'USJ 57W', 'state': 'Putrajaya', 'latitude': '3.117134', 'longitude': '101.615027'},
{'store_id': 4, 'store_name': 'G Coffee @ Kampung Changkat', 'street': 'Jln 6/6A', 'postal_code': '62941', 'city': 'Kampung Changkat', 'state': 'Putrajaya', 'latitude': '2.914594', 'longitude': '101.704486'},
{'store_id': 5, 'store_name': 'G Coffee @ Seksyen 21', 'street': 'Jalan Anson 4k', 'postal_code': '62595', 'city': 'Seksyen 21', 'state': 'Putrajaya', 'latitude': '2.937599', 'longitude': '101.698478'},

{'store_id': 6, 'store_name': 'G Coffee @ Alam Tun Hussein Onn', 'street': 'Jln Pasar Besar 63s', 'postal_code': '63518', 'city': 'Alam Tun Hussein Onn', 'state': 'Selangor Darul Ehsan', 'latitude': '3.279175', 'longitude': '101.784923'},
{'store_id': 7, 'store_name': 'G Coffee @ Damansara Saujana', 'street': 'Jln 8/74', 'postal_code': '65438', 'city': 'Damansara Saujana', 'state': 'Selangor Darul Ehsan', 'latitude': '3.22081', 'longitude': '101.58459'},
{'store_id': 8, 'store_name': 'G Coffee @ Bandar Seri Mulia', 'street': 'Jalan Wisma Putra', 'postal_code': '58621', 'city': 'Bandar Seri Mulia', 'state': 'Kuala Lumpur', 'latitude': '3.140674', 'longitude': '101.706562'},
{'store_id': 9, 'store_name': 'G Coffee @ PJS8', 'street': 'Jalan 7/3o', 'postal_code': '62418', 'city': 'PJS8', 'state': 'Putrajaya', 'latitude': '2.952444', 'longitude': '101.702623'},
{'store_id': 10, 'store_name': 'G Coffee @ Taman Damansara', 'street': 'Jln 2', 'postal_code': '67102', 'city': 'Taman Damansara', 'state': 'Selangor Darul Ehsan', 'latitude': '3.497178', 'longitude': '101.595271'},
]


class JoinTransactionsWithStores:
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, shutting down JoinItemsWithMenu")
        self.running = False

    def start(self):
        logging.info("Starting JoinItemsWithMenu.")
        self.__join(TRANSACTIONS, STORES)
        while self.running:
            sleep(1)

    def __join(self, transactions, stores):
        joined = []
        for transaction in transactions:
            for store in stores:
                if transaction.get('store_id') == store.get('store_id'):
                    joined.append({**transaction, **store})
        self.running = False
        self.__produce_output(joined)

    def __produce_output(self, joined_items):
        for item in joined_items:
            print(f"{item['store_id']}: {item['store_name']} ({item['transaction_id']})")

