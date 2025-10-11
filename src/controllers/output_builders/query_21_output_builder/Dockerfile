FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/shared /shared
COPY src/middleware /middleware

COPY src/controllers/shared /controllers/shared
COPY src/controllers/output_builders/shared /controllers/output_builders/shared
COPY src/controllers/output_builders/query_21_output_builder /controllers/output_builders/query_21_output_builder

ENTRYPOINT ["/bin/sh"]