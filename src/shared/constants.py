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

MIT_FOLDER_NAME = "menu_items"
STR_FOLDER_NAME = "stores"
TIT_FOLDER_NAME = "transaction_items"
TRN_FOLDER_NAME = "transactions"
USR_FOLDER_NAME = "users"
QRS_FOLDER_NAME = "query_results"

# message middleware constants

MIT_CLEANER_QUEUE_PREFIX = "menu-items-cleaner-queue"
STR_CLEANER_QUEUE_PREFIX = "stores-cleaner-queue"
TIT_CLEANER_QUEUE_PREFIX = "transaction-items-cleaner-queue"
TRN_CLEANER_QUEUE_PREFIX = "transactions-cleaner-queue"
USR_CLEANER_QUEUE_PREFIX = "users-cleaner-queue"

CLEANED_TIT_QUEUE_PREFIX = "cleaned-transaction-items"
CLEANED_TRN_QUEUE_PREFIX = "cleaned-transactions"
CLEANED_USR_QUEUE_PREFIX = "cleaned-users"

CLEANED_MIT_EXCHANGE_PREFIX = "cleaned-menu-items-exchange"

CLEANED_MIT_ROUTING_KEYS = ["cleaned-menu-items-routing-key"]

QRS_QUEUE_PREFIX = "query-results-queue"
