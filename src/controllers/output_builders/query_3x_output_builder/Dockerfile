FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/output_builders/query_output_builder /controllers/output_builders/query_output_builder
COPY src/controllers/output_builders/query_3x_output_builder /controllers/output_builders/query_3x_output_builder
COPY src/shared /shared
COPY src/middleware /middleware

ENTRYPOINT ["/bin/sh"]