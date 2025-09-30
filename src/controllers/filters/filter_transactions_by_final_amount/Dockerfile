FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/filters/filter_transactions_by_final_amount /controllers/filters/filter_transactions_by_final_amount
COPY src/shared /shared
COPY src/middleware /middleware

ENTRYPOINT ["/bin/sh"]