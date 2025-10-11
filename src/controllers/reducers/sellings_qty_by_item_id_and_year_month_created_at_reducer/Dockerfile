
FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/shared /shared
COPY src/middleware /middleware

COPY src/controllers/shared /controllers/shared
COPY src/controllers/reducers/shared /controllers/reducers/shared
COPY src/controllers/reducers/sellings_qty_by_item_id_and_year_month_created_at_reducer /controllers/reducers/sellings_qty_by_item_id_and_year_month_created_at_reducer

ENTRYPOINT ["/bin/sh"]