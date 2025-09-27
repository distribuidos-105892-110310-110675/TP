import signal
import logging
from time import sleep

USERS = [
    {"user_id": "17585", "gender": "female", "birthdate": "2003-01-06", "registered_at": "2024-01-02 10:42:29", 'store_id': 1},
    {"user_id": "17586", "gender": "female", "birthdate": "2002-03-29", "registered_at": "2024-01-02 10:42:50", 'store_id': 2},
    {"user_id": "17587", "gender": "male",   "birthdate": "1979-05-28", "registered_at": "2024-01-02 10:42:55", 'store_id': 3},
    {"user_id": "17588", "gender": "female", "birthdate": "1979-09-14", "registered_at": "2024-01-02 10:43:37", 'store_id': 10},
    {"user_id": "17589", "gender": "female", "birthdate": "1981-03-22", "registered_at": "2024-01-02 10:46:17", 'store_id': 10},
    {"user_id": "17590", "gender": "male",   "birthdate": "1986-07-20", "registered_at": "2024-01-02 10:46:41", 'store_id': 5},
    {"user_id": "17591", "gender": "female", "birthdate": "1997-05-20", "registered_at": "2024-01-02 10:48:52", 'store_id': 2},
    {"user_id": "17592", "gender": "male",   "birthdate": "2008-07-05", "registered_at": "2024-01-02 10:50:38", 'store_id': 7},
    {"user_id": "17593", "gender": "female", "birthdate": "1986-02-20", "registered_at": "2024-01-02 10:54:15", 'store_id': 8},
    {"user_id": "17594", "gender": "female", "birthdate": "2005-06-23", "registered_at": "2024-01-02 10:59:33", 'store_id': 1},
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


class JoinUsersWithStores():
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, shutting down JoinTransactionsWithUsers")
        self.running = False

    def start(self):
        logging.info("Starting JoinTransactionsWithUsers.")
        self.__join(STORES, USERS)
        while self.running:
            sleep(1)

    def __join(self, stores, users):
        joined_items = []
        for user in users:
            for store in stores:
                if user.get('store_id') == store.get('store_id'):
                    joined_items.append({**user, **store})
        self.running = False
        self.__produce_output(joined_items)

    def __produce_output(self, joined_items):
        for item in joined_items:
            print(f"{item['store_name']} ({item['store_id']}): {item['birthdate']}")