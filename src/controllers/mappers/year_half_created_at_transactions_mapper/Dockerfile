FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/shared /shared
COPY src/middleware /middleware

COPY src/controllers/shared /controllers/shared
COPY src/controllers/mappers/shared /controllers/mappers/shared
COPY src/controllers/mappers/year_half_created_at_transactions_mapper /controllers/mappers/year_half_created_at_transactions_mapper

ENTRYPOINT ["/bin/sh"]
