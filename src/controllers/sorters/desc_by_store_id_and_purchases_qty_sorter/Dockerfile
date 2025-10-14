FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/shared /shared
COPY src/middleware /middleware

COPY src/controllers/shared /controllers/shared
COPY src/controllers/sorters/shared /controllers/sorters/shared
COPY src/controllers/sorters/desc_by_store_id_and_purchases_qty_sorter/ /controllers/sorters/desc_by_store_id_and_purchases_qty_sorter/

ENTRYPOINT ["/bin/sh"]