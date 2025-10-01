FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/sorters/sort_desc_by_year_month_created_at_and_sellings_qty /controllers/sorters/sort_desc_by_year_month_created_at_and_sellings_qty
COPY src/shared /shared
COPY src/middleware /middleware

ENTRYPOINT ["/bin/sh"]