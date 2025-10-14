FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/shared /shared
COPY src/middleware /middleware

COPY src/controllers/shared /controllers/shared
COPY src/controllers/sorters/shared /controllers/sorters/shared
COPY src/controllers/sorters/desc_by_year_month_created_at_and_profit_sum_sorter /controllers/sorters/desc_by_year_month_created_at_and_profit_sum_sorter

ENTRYPOINT ["/bin/sh"]