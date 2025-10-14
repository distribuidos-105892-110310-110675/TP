FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/shared /shared
COPY src/middleware /middleware

COPY src/controllers/shared /controllers/shared
COPY src/controllers/joiners/shared /controllers/joiners/shared
COPY src/controllers/joiners/transaction_items_with_menu_items_joiner/shared /controllers/joiners/transaction_items_with_menu_items_joiner/shared
COPY src/controllers/joiners/transaction_items_with_menu_items_joiner/transaction_items_with_menu_items_query_21_joiner/ /controllers/joiners/transaction_items_with_menu_items_joiner/transaction_items_with_menu_items_query_21_joiner/

ENTRYPOINT ["/bin/sh"]