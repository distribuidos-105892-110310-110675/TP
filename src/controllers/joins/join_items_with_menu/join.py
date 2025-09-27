import signal
import logging
from time import sleep

MENU_ITEMS = [
    {'item_id': 1, 'item_name': 'Espresso', 'category': 'coffee', 'price': 6.0, 'is_seasonal': False, 'available_from': '', 'available_to': ''},
    {'item_id': 2, 'item_name': 'Americano', 'category': 'coffee', 'price': 7.0, 'is_seasonal': False, 'available_from': '', 'available_to': ''},
    {'item_id': 3, 'item_name': 'Latte', 'category': 'coffee', 'price': 8.0, 'is_seasonal': False, 'available_from': '', 'available_to': ''},
    {'item_id': 4, 'item_name': 'Cappuccino', 'category': 'coffee', 'price': 8.0, 'is_seasonal': False, 'available_from': '', 'available_to': ''},
    {'item_id': 5, 'item_name': 'Flat White', 'category': 'coffee', 'price': 9.0, 'is_seasonal': False, 'available_from': '', 'available_to': ''},
    {'item_id': 6, 'item_name': 'Mocha', 'category': 'coffee', 'price': 9.5, 'is_seasonal': False, 'available_from': '', 'available_to': ''},
    {'item_id': 7, 'item_name': 'Hot Chocolate', 'category': 'non-coffee', 'price': 9.0, 'is_seasonal': False, 'available_from': '', 'available_to': ''},
    {'item_id': 8, 'item_name': 'Matcha Latte', 'category': 'non-coffee', 'price': 10.0, 'is_seasonal': False, 'available_from': '', 'available_to': ''},
]

ITEMS = [
{"transaction_id": "ac6f851c-649f-42fb-a606-72be0fdcf8d2", "item_id": "6", "quantity": "1", "unit_price": "9.5", "subtotal": "9.5", "created_at": "2024-01-01 10:06:50"},
    {"transaction_id": "ac6f851c-649f-42fb-a606-72be0fdcf8d2", "item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-01-01 10:06:50"},
    {"transaction_id": "ac6f851c-649f-42fb-a606-72be0fdcf8d2", "item_id": "8", "quantity": "3", "unit_price": "10.0", "subtotal": "30.0", "created_at": "2024-01-01 10:06:50"},
    {"transaction_id": "d7856e66-c613-45c4-b9ca-7a3cec6c6db3", "item_id": "2", "quantity": "3", "unit_price": "7.0", "subtotal": "21.0", "created_at": "2024-01-01 10:06:52"},
    {"transaction_id": "d7856e66-c613-45c4-b9ca-7a3cec6c6db3", "item_id": "6", "quantity": "1", "unit_price": "9.5", "subtotal": "9.5", "created_at": "2024-01-01 10:06:52"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "1", "quantity": "1", "unit_price": "6.0", "subtotal": "6.0", "created_at": "2024-01-01 10:06:53"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-01-01 10:06:53"},
    {"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "4", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-01-01 10:06:53"},
{"transaction_id": "78015742-1f8b-4f9c-bde2-0e68b822890c", "item_id": "7", "quantity": "3", "unit_price": "8.0", "subtotal": "24.0", "created_at": "2024-01-01 10:06:53"},
]

class JoinItemsWithMenu:
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGTERM, self.__handle_sigterm_signal)

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, shutting down JoinItemsWithMenu")
        self.running = False

    def start(self):
        logging.info("Starting JoinItemsWithMenu.")
        self.__join(MENU_ITEMS, ITEMS)
        while self.running:
            sleep(1)

    def __join(self, menu_items, items):
        joined_items = []
        for item in items:
            for m_item in menu_items:
                if int(item.get('item_id')) == m_item.get('item_id'):
                    joined_items.append({**item, **m_item})
        self.running = False
        self.__produce_output(joined_items)

    def __produce_output(self, joined_items):
        for item in joined_items:
            print(f"{item['item_id']}: {item['item_name']} ({item['category']})")