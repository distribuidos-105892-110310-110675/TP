FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/mappers/map_month_semester_transactions /controllers/mappers/map_month_semester_transactions
COPY src/shared /shared
COPY src/middleware /middleware

ENV PYTHONPATH=/

ENTRYPOINT ["/bin/sh"]
