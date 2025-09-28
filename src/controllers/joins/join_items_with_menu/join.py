import signal
import logging
from shared import communication_protocol

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
        self.menu_items = []
        self.transaction_items = []
        self.received_all_transaction_items = False
        self.receive_all_menu_items = False

    def __handle_sigterm_signal(self, signal, frame):
        logging.info("Received SIGTERM, shutting down JoinItemsWithMenu")
        self.running = False

    def start(self):
        chunk_size = len(ITEMS) // 2
        i = 0
        while i < len(ITEMS):
            new_chunk = ITEMS[i:i + chunk_size]
            self.__receive_data(communication_protocol.encode_transaction_items_batch_message(new_chunk))
            i += chunk_size
        self.__receive_data(communication_protocol.encode_eof_message(communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE))
        chunk_size = len(MENU_ITEMS) // 2
        i = 0
        while i < len(MENU_ITEMS):
            new_chunk = MENU_ITEMS[i:i + chunk_size]
            self.__receive_data(communication_protocol.encode_menu_items_batch_message(new_chunk))
            i += chunk_size
        self.__receive_data(communication_protocol.encode_eof_message(communication_protocol.MENU_ITEMS_BATCH_MSG_TYPE))
        self.__join()

    def __receive_data(self, chunk):
        msg_type = communication_protocol.decode_message_type(chunk)
        if msg_type == communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE:
            transaction_items = communication_protocol.decode_transaction_items_batch_message(chunk)
            for ti in transaction_items:
                self.transaction_items.append(ti)
        elif msg_type == communication_protocol.MENU_ITEMS_BATCH_MSG_TYPE:
            menu_items = communication_protocol.decode_menu_items_batch_message(chunk)
            for mi in menu_items:
                self.menu_items.append(mi)
        elif msg_type == communication_protocol.EOF:
            body = communication_protocol.decode_eof_message(chunk)
            if body == communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE:
                self.received_all_transaction_items = True
            elif body == communication_protocol.MENU_ITEMS_BATCH_MSG_TYPE:
                self.received_all_menu_items = True
            else:
                logging.error("Received unknown message type")
        else:
            logging.error("Received unknown message type")

    def __join(self):
        joined_items = []
        for item in self.transaction_items:
            for m_item in self.menu_items:
                if int(item.get('item_id')) == int(m_item.get('item_id')):
                    joined_items.append({**item, **m_item})
        self.running = False
        self.__produce_output(joined_items)

    def __produce_output(self, joined_items):
        for item in joined_items:
            logging.info(f"{item['item_id']}: {item['item_name']} ({item['category']})")