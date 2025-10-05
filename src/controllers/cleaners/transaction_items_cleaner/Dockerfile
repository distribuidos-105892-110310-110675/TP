FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/shared /shared
COPY src/middleware /middleware
COPY src/controllers/controller.py /controllers/controller.py

COPY src/controllers/cleaners/cleaner.py /controllers/cleaners/cleaner.py
COPY src/controllers/cleaners/transaction_items_cleaner /controllers/cleaners/transaction_items_cleaner

ENTRYPOINT ["/bin/sh"]