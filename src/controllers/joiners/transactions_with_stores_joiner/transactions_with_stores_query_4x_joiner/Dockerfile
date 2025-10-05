FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/shared /shared
COPY src/middleware /middleware
COPY src/controllers/controller.py /controllers/controller.py

COPY src/controllers/joiners/joiner.py /controllers/joiners/joiner.py
COPY src/controllers/joiners/transactions_with_stores_joiner/transactions_with_stores_joiner.py /controllers/joiners/transactions_with_stores_joiner/transactions_with_stores_joiner.py
COPY src/controllers/joiners/transactions_with_stores_joiner/transactions_with_stores_query_4x_joiner/ /controllers/joiners/transactions_with_stores_joiner/transactions_with_stores_query_4x_joiner/

ENTRYPOINT ["/bin/sh"]