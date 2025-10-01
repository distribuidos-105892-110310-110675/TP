# the fixed length of the message type prefix in the protocol
MESSAGE_TYPE_LENGTH = 3

# messages types
QUERY_MSG_TYPE = "QRY"
ACK_MSG_TYPE = "ACK"

MENU_ITEMS_BATCH_MSG_TYPE = "MIT"
STORES_BATCH_MSG_TYPE = "STR"
TRANSACTION_ITEMS_BATCH_MSG_TYPE = "TIT"
TRANSACTIONS_BATCH_MSG_TYPE = "TRN"
USERS_BATCH_MSG_TYPE = "USR"

QUERY_RESULT_1X_MSG_TYPE = "Q1X"
QUERY_RESULT_21_MSG_TYPE = "Q21"
QUERY_RESULT_22_MSG_TYPE = "Q22"
QUERY_RESULT_3X_MSG_TYPE = "Q3X"
QUERY_RESULT_4X_MSG_TYPE = "Q4X"

# delimiters & separators
MSG_START_DELIMITER = "["
MSG_END_DELIMITER = "]"

BATCH_START_DELIMITER = "{"
BATCH_END_DELIMITER = "}"
BATCH_ROW_SEPARATOR = ";"
ROW_FIELD_SEPARATOR = ","

# payload
EOF = "EOF"

# ============================= PRIVATE - DECODE ============================== #


def __assert_message_format(message: str, expected_message_type: str) -> None:
    received_message_type = decode_message_type(message)
    if received_message_type != expected_message_type:
        raise ValueError(
            f"Unexpected message type. Expected: {expected_message_type}, Received: {received_message_type}",
        )

    if not (
        message.startswith(expected_message_type + MSG_START_DELIMITER)
        and message.endswith(MSG_END_DELIMITER)
    ):
        raise ValueError("Unexpected message format")


def __decode_field(key_value_pair: str) -> tuple[str, str]:
    key, value = key_value_pair.split(":", 1)
    key = key.strip('"')
    value = value.strip('"')
    return key, value


def __decode_row(encoded_row: str) -> dict[str, str]:
    encoded_row = encoded_row.strip(BATCH_START_DELIMITER)
    encoded_row = encoded_row.strip(BATCH_END_DELIMITER)

    key_value_pairs = encoded_row.split(ROW_FIELD_SEPARATOR)

    row = {}
    for key_value_pair in key_value_pairs:
        key, value = __decode_field(key_value_pair)
        row[key] = value

    return row


def __decode_batch_message_with_type(
    message: str, message_type: str
) -> list[dict[str, str]]:
    __assert_message_format(message, message_type)
    return decode_batch_message(message)


# ============================= DECODE ============================== #


def get_message_payload(message: str) -> str:
    payload = message[MESSAGE_TYPE_LENGTH:]

    payload = payload[len(MSG_START_DELIMITER) : -len(MSG_END_DELIMITER)]

    return payload


def decode_is_empty_message(message: str) -> bool:
    payload = get_message_payload(message)
    return len(payload) == 0


def decode_message_type(message: str) -> str:
    if len(message) < MESSAGE_TYPE_LENGTH:
        raise ValueError(
            f"Message too short to contain a valid message type: {message}"
        )
    return message[:MESSAGE_TYPE_LENGTH]


def decode_batch_message(message: str) -> list[dict[str, str]]:
    payload = get_message_payload(message)
    encoded_rows = payload.split(BATCH_ROW_SEPARATOR)
    decoded_rows = []

    for encoded_row in encoded_rows:
        bet = __decode_row(encoded_row)
        decoded_rows.append(bet)

    return decoded_rows


def decode_menu_items_batch_message(message: str) -> list[dict[str, str]]:
    return __decode_batch_message_with_type(message, MENU_ITEMS_BATCH_MSG_TYPE)


def decode_stores_batch_message(message: str) -> list[dict[str, str]]:
    return __decode_batch_message_with_type(message, STORES_BATCH_MSG_TYPE)


def decode_transaction_items_batch_message(message: str) -> list[dict[str, str]]:
    return __decode_batch_message_with_type(message, TRANSACTION_ITEMS_BATCH_MSG_TYPE)


def decode_transactions_batch_message(message: str) -> list[dict[str, str]]:
    return __decode_batch_message_with_type(message, TRANSACTIONS_BATCH_MSG_TYPE)


def decode_users_batch_message(message: str) -> list[dict[str, str]]:
    return __decode_batch_message_with_type(message, USERS_BATCH_MSG_TYPE)


def decode_eof_message(message: str) -> str:
    __assert_message_format(message, EOF)
    return get_message_payload(message)


# ============================= PRIVATE - ENCODE ============================== #


def __encode_message(message_type: str, payload: str) -> str:
    encoded_payload = message_type
    encoded_payload += MSG_START_DELIMITER
    encoded_payload += payload
    encoded_payload += MSG_END_DELIMITER
    return encoded_payload


# ============================= PRIVATE - ENCODE BATCH ============================== #


def __encode_field(key: str, value: str) -> str:
    return f'"{key}":"{value}"'


def __encode_row(row: dict[str, str]) -> str:
    encoded_fields = [__encode_field(key, value) for key, value in row.items()]
    ecoded_row = ROW_FIELD_SEPARATOR.join(encoded_fields)
    return BATCH_START_DELIMITER + ecoded_row + BATCH_END_DELIMITER


# ============================= PUBLIC ============================== #


def encode_ack_message(message: str) -> str:
    return __encode_message(ACK_MSG_TYPE, message)


def encode_batch_message(batch_msg_type: str, batch: list[dict[str, str]]) -> str:
    encoded_rows = []

    for item in batch:
        encoded_row = __encode_row(item)
        encoded_rows.append(encoded_row)

    encoded_payload = BATCH_ROW_SEPARATOR.join(encoded_rows)
    return __encode_message(batch_msg_type, encoded_payload)


def encode_menu_items_batch_message(
    menu_items_batch: list[dict[str, str]],
) -> str:
    return encode_batch_message(
        MENU_ITEMS_BATCH_MSG_TYPE,
        menu_items_batch,
    )


def encode_stores_batch_message(
    stores_batch: list[dict[str, str]],
) -> str:
    return encode_batch_message(
        STORES_BATCH_MSG_TYPE,
        stores_batch,
    )


def encode_transaction_items_batch_message(
    transaction_items_batch: list[dict[str, str]],
) -> str:
    return encode_batch_message(
        TRANSACTION_ITEMS_BATCH_MSG_TYPE,
        transaction_items_batch,
    )


def encode_transactions_batch_message(
    transactions_batch: list[dict[str, str]],
) -> str:
    return encode_batch_message(
        TRANSACTIONS_BATCH_MSG_TYPE,
        transactions_batch,
    )


def encode_users_batch_message(
    users_batch: list[dict[str, str]],
) -> str:
    return encode_batch_message(
        USERS_BATCH_MSG_TYPE,
        users_batch,
    )


def encode_eof_message(message_type: str) -> str:
    return __encode_message(EOF, message_type)
