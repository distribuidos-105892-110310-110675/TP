from shared import communication_protocol

# common constants

KiB = 1024

# common tags

MENU_ITEMS = communication_protocol.MENU_ITEMS_BATCH_MSG_TYPE
STORES = communication_protocol.STORES_BATCH_MSG_TYPE
TRANSACTION_ITEMS = communication_protocol.TRANSACTION_ITEMS_BATCH_MSG_TYPE
TRANSACTIONS = communication_protocol.TRANSACTIONS_BATCH_MSG_TYPE
USERS = communication_protocol.USERS_BATCH_MSG_TYPE

QUERY_RESULT_1X = communication_protocol.QUERY_RESULT_1X_MSG_TYPE
QUERY_RESULT_21 = communication_protocol.QUERY_RESULT_21_MSG_TYPE
QUERY_RESULT_22 = communication_protocol.QUERY_RESULT_22_MSG_TYPE
QUERY_RESULT_3X = communication_protocol.QUERY_RESULT_3X_MSG_TYPE
QUERY_RESULT_4X = communication_protocol.QUERY_RESULT_4X_MSG_TYPE

QUEUE_PREFIX = "queue_prefix_name"
WORKERS_AMOUNT = "workers_amount"

# folder names

MIT_FOLDER_NAME = "menu_items"
STR_FOLDER_NAME = "stores"
TIT_FOLDER_NAME = "transaction_items"
TRN_FOLDER_NAME = "transactions"
USR_FOLDER_NAME = "users"
QRS_FOLDER_NAME = "query_results"

# message middleware constants

DIRTY_MIT_QUEUE_PREFIX = "dirty-menu-items-queue"
DIRTY_STR_QUEUE_PREFIX = "dirty-stores-queue"
DIRTY_TIT_QUEUE_PREFIX = "dirty-transaction-items-queue"
DIRTY_TRN_QUEUE_PREFIX = "dirty-transactions-queue"
DIRTY_USR_QUEUE_PREFIX = "dirty-users-queue"

CLEANED_TIT_QUEUE_PREFIX = "cleaned-transaction-items"
CLEANED_TRN_QUEUE_PREFIX = "cleaned-transactions"
CLEANED_USR_QUEUE_PREFIX = "cleaned-users"
CLEANED_MIT_EXCHANGE_PREFIX = "cleaned-menu-items-exchange"
CLEANED_STR_EXCHANGE_PREFIX = "cleaned-stores-exchange"
CLEANED_MIT_ROUTING_KEY_PREFIX = "cleaned-menu-items-routing-key"
CLEANED_STR_ROUTING_KEY_PREFIX = "cleaned-stores-routing-key"


FILTERED_TRN_BY_YEAR_EXCHANGE_PREFIX = "filtered-transactions-by-year-exchange"
FILTERED_TRN_BY_YEAR_ROUTING_KEY_PREFIX = "filtered-transactions-by-year-routing-key"

FILTERED_TIT_QUEUE_PREFIX = "filtered-transaction-items-by-year-queue"

FILTERED_TRN_BY_YEAR__HOUR_EXCHANGE_PREFIX = (
    "filtered-transactions-by-year-&-hour-exchange"
)
FILTERED_TRN_BY_YEAR__HOUR_ROUTING_KEY_PREFIX = (
    "filtered-transactions-by-year-&-hour-routing-key"
)
FILTERED_TRN_BY_YEAR__HOUR__FINAL_AMOUNT_QUEUE_PREFIX = (
    "filtered-transactions-by-year-&-time-&-final-amount"
)
SORTED_DESC_SELLING_QTY_BY_YEAR_MONTH__ITEM_NAME_QUEUE_PREFIX = (
    "sorted-desc-selling-qty-by-year-month-&-item-name"
)
SORTED_DESC_PROFIT_SUM_BY_YEAR_MONTH__ITEM_NAME_QUEUE_PREFIX = (
    "sorted-desc-profit-sum-by-year-month-&-item-name"
)
TPV_BY_HALF_YEAR_CREATED_AT__STORE_NAME_QUEUE_PREFIX = (
    "tpv-by-half-year-created-at-&-store-name"
)
SORTED_DESC_USR_PURCHASES_BY_USR_BIRTHDATE__STORE_NAME_QUEUE_PREFIX = (
    "sorted-desc-user-purchases-by-user-birthdate-&-store-name"
)

QRS_QUEUE_PREFIX = "query-results-queue"
