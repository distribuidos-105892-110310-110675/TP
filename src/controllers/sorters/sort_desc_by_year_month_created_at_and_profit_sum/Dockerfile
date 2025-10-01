FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/sorters/sort_desc_by_year_month_created_at_and_profit_sum /controllers/sorters/sort_desc_by_year_month_created_at_and_profit_sum
COPY src/shared /shared
COPY src/middleware /middleware

ENTRYPOINT ["/bin/sh"]