FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/sorters/sort_desc_by_store_id_and_purchases_qty /controllers/sorters/sort_desc_by_store_id_and_purchases_qty
COPY src/shared /shared
COPY src/middleware /middleware

ENTRYPOINT ["/bin/sh"]