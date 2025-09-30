FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/cleaners/transaction_items_cleaner /controllers/cleaners/transaction_items_cleaner
COPY src/shared /shared
COPY src/middleware /middleware

ENTRYPOINT ["/bin/sh"]