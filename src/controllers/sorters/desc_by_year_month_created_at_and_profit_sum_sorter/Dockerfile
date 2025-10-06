FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/shared /shared
COPY src/middleware /middleware
COPY src/controllers/controller.py /controllers/controller.py

COPY src/controllers/sorters/sorter.py /controllers/sorters/sorter.py
COPY src/controllers/sorters/desc_by_year_month_created_at_and_profit_sum_sorter /controllers/sorters/desc_by_year_month_created_at_and_profit_sum_sorter

ENTRYPOINT ["/bin/sh"]