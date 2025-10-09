# the fixed length of the message type prefix in the protocol
MESSAGE_TYPE_LENGTH = 3

# messages types
HANDSHAKE_MSG_TYPE = "HSK"

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
SESSION_ID_DELIMITER = "|"

MSG_START_DELIMITER = "["
MSG_END_DELIMITER = "]"

BATCH_START_DELIMITER = "{"
BATCH_END_DELIMITER = "}"
BATCH_ROW_SEPARATOR = ";"
ROW_FIELD_SEPARATOR = ","

# payload
ALL_QUERIES = "Q1X;Q21;Q22;Q3X;Q4X"
EOF = "EOF"

# ============================= PRIVATE - DECODE ============================== #


def _assert_message_format(expected_message_type: str, message: str) -> None:
    received_message_type = get_message_type(message)
    if received_message_type != expected_message_type:
        raise ValueError(
            f"Unexpected message type. Expected: {expected_message_type}, Received: {received_message_type}",
        )


def _decode_field(key_value_pair: str) -> tuple[str, str]:
    key, value = key_value_pair.split(":", 1)
    key = key.strip('"')
    value = value.strip('"')
    return key, value


def _decode_row(encoded_row: str) -> dict[str, str]:
    encoded_row = encoded_row.strip(BATCH_START_DELIMITER)
    encoded_row = encoded_row.strip(BATCH_END_DELIMITER)

    key_value_pairs = encoded_row.split(ROW_FIELD_SEPARATOR)

    row = {}
    for key_value_pair in key_value_pairs:
        key, value = _decode_field(key_value_pair)
        row[key] = value

    return row


def _decode_batch_message_with_type(
    message_type: str, message: str
) -> list[dict[str, str]]:
    _assert_message_format(message_type, message)
    return decode_batch_message(message)


# ============================= DECODE ============================== #


def get_message_session_id(message: str) -> str:
    session_id_start = message.index(SESSION_ID_DELIMITER)
    session_id_end = message.index(MSG_START_DELIMITER, session_id_start)

    session_id = message[session_id_start + 1 : session_id_end]

    return session_id


def get_message_payload(message: str) -> str:
    payload_start = message.index(MSG_START_DELIMITER)
    payload_end = message.index(MSG_END_DELIMITER, payload_start)

    payload = message[payload_start + 1 : payload_end]

    return payload


def message_without_payload(message: str) -> bool:
    payload = get_message_payload(message)
    return len(payload) == 0


def get_message_type(message: str) -> str:
    if len(message) < MESSAGE_TYPE_LENGTH:
        raise ValueError(
            f"Message too short to contain a valid message type: {message}"
        )
    return message[:MESSAGE_TYPE_LENGTH]


def decode_handshake_message(message: str) -> tuple[str, str]:
    _assert_message_format(HANDSHAKE_MSG_TYPE, message)
    return get_message_session_id(message), get_message_payload(message)


def decode_batch_message(message: str) -> list[dict[str, str]]:
    payload = get_message_payload(message)
    encoded_rows = payload.split(BATCH_ROW_SEPARATOR)
    decoded_rows = []

    for encoded_row in encoded_rows:
        bet = _decode_row(encoded_row)
        decoded_rows.append(bet)

    return decoded_rows


def decode_menu_items_batch_message(message: str) -> list[dict[str, str]]:
    return _decode_batch_message_with_type(MENU_ITEMS_BATCH_MSG_TYPE, message)


def decode_stores_batch_message(message: str) -> list[dict[str, str]]:
    return _decode_batch_message_with_type(STORES_BATCH_MSG_TYPE, message)


def decode_transaction_items_batch_message(message: str) -> list[dict[str, str]]:
    return _decode_batch_message_with_type(TRANSACTION_ITEMS_BATCH_MSG_TYPE, message)


def decode_transactions_batch_message(message: str) -> list[dict[str, str]]:
    return _decode_batch_message_with_type(TRANSACTIONS_BATCH_MSG_TYPE, message)


def decode_users_batch_message(message: str) -> list[dict[str, str]]:
    return _decode_batch_message_with_type(USERS_BATCH_MSG_TYPE, message)


def decode_eof_message(message: str) -> str:
    _assert_message_format(EOF, message)
    return get_message_payload(message)


# ============================= PRIVATE - ENCODE ============================== #


def _encode_message(message_type: str, user_id: str, payload: str) -> str:
    encoded_payload = message_type
    encoded_payload += SESSION_ID_DELIMITER
    encoded_payload += user_id
    encoded_payload += MSG_START_DELIMITER
    encoded_payload += payload
    encoded_payload += MSG_END_DELIMITER
    return encoded_payload


def _encode_field(key: str, value: str) -> str:
    return f'"{key}":"{value}"'


def _encode_row(row: dict[str, str]) -> str:
    encoded_fields = [_encode_field(key, value) for key, value in row.items()]
    ecoded_row = ROW_FIELD_SEPARATOR.join(encoded_fields)
    return BATCH_START_DELIMITER + ecoded_row + BATCH_END_DELIMITER


# ============================= ENCODE ============================== #


def encode_handshake_message(id: str, payload: str) -> str:
    return _encode_message(HANDSHAKE_MSG_TYPE, id, payload)


def encode_batch_message(
    batch_msg_type: str,
    session_id: str,
    batch: list[dict[str, str]],
) -> str:
    encoded_rows = []

    for item_batch in batch:
        encoded_row = _encode_row(item_batch)
        encoded_rows.append(encoded_row)

    encoded_payload = BATCH_ROW_SEPARATOR.join(encoded_rows)
    return _encode_message(batch_msg_type, session_id, encoded_payload)


def encode_menu_items_batch_message(
    session_id: str,
    menu_items_batch: list[dict[str, str]],
) -> str:
    return encode_batch_message(
        MENU_ITEMS_BATCH_MSG_TYPE,
        session_id,
        menu_items_batch,
    )


def encode_stores_batch_message(
    session_id: str,
    stores_batch: list[dict[str, str]],
) -> str:
    return encode_batch_message(
        STORES_BATCH_MSG_TYPE,
        session_id,
        stores_batch,
    )


def encode_transaction_items_batch_message(
    session_id: str,
    transaction_items_batch: list[dict[str, str]],
) -> str:
    return encode_batch_message(
        TRANSACTION_ITEMS_BATCH_MSG_TYPE,
        session_id,
        transaction_items_batch,
    )


def encode_transactions_batch_message(
    session_id: str,
    transactions_batch: list[dict[str, str]],
) -> str:
    return encode_batch_message(
        TRANSACTIONS_BATCH_MSG_TYPE,
        session_id,
        transactions_batch,
    )


def encode_users_batch_message(
    session_id: str,
    users_batch: list[dict[str, str]],
) -> str:
    return encode_batch_message(
        USERS_BATCH_MSG_TYPE,
        session_id,
        users_batch,
    )


def encode_eof_message(session_id: str, message_type: str) -> str:
    return _encode_message(EOF, session_id, message_type)
