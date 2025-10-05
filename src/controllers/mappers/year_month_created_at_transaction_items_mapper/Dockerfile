FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/shared /shared
COPY src/middleware /middleware
COPY src/controllers/controller.py /controllers/controller.py

COPY src/controllers/mappers/mapper.py /controllers/mappers/mapper.py
COPY src/controllers/mappers/year_month_created_at_transaction_items_mapper /controllers/mappers/year_month_created_at_transaction_items_mapper

ENTRYPOINT ["/bin/sh"]