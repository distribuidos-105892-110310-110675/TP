
FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/reduces/count_transaction_items /controllers/reduces/count_transaction_items
COPY src/shared /shared
COPY src/middleware /middleware

ENTRYPOINT ["/bin/sh"]