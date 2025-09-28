from shared import communication_protocol

# common constants

KiB = 1024

# common tags

MENU_ITEMS = communication_protocol.MENU_ITEMS_BATCH_MSG_TYPE
STORES = communication_protocol.STORES_BATCH_MSG_TYPE
TRANSACTION_ITEMS = communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE
TRANSACTIONS = communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE
USERS = communication_protocol.USERS_BATCH_MSG_TYPE

QUERY_RESULT_1 = communication_protocol.QUERY_RESULT_1_MSG_TYPE
QUERY_RESULT_2_1 = communication_protocol.QUERY_RESULT_2_1_MSG_TYPE
QUERY_RESULT_2_2 = communication_protocol.QUERY_RESULT_2_2_MSG_TYPE
QUERY_RESULT_3 = communication_protocol.QUERY_RESULT_3_MSG_TYPE
QUERY_RESULT_4 = communication_protocol.QUERY_RESULT_4_MSG_TYPE

QUEUE_PREFIX_NAME = "queue_prefix_name"
WORKERS_AMOUNT = "workers_amount"

# folder names

MENU_ITEMS_FOLDER_NAME = "menu_items"
STORES_FOLDER_NAME = "stores"
TRANSACTION_ITEMS_FOLDER_NAME = "transaction_items"
TRANSACTIONS_FOLDER_NAME = "transactions"
USERS_FOLDER_NAME = "users"
RESULTS_FOLDER_NAME = "query_results"
