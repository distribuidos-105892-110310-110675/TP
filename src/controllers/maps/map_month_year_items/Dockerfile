FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/maps/map_month_year_items /controllers/maps/map_month_year_items
COPY src/shared /shared
COPY src/middleware /middleware

ENTRYPOINT ["/bin/sh"]