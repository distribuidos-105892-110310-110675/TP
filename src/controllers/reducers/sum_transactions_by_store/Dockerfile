FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/reducers/sum_transactions_by_store /controllers/reducers/sum_transactions_by_store
COPY src/shared /shared
COPY src/middleware /middleware

ENTRYPOINT ["/bin/sh"]