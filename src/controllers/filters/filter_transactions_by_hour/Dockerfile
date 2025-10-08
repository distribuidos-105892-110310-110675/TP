FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/shared /shared
COPY src/middleware /middleware
COPY src/controllers/controller.py /controllers/controller.py

COPY src/controllers/filters/filter.py /controllers/filters/filter.py
COPY src/controllers/filters/filter_transactions_by_hour /controllers/filters/filter_transactions_by_hour

ENTRYPOINT ["/bin/sh"]