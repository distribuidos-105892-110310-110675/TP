FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/shared /shared
COPY src/middleware /middleware
COPY src/controllers/controller.py /controllers/controller.py

COPY src/controllers/output_builders/query_output_builder.py /controllers/output_builders/query_output_builder.py
COPY src/controllers/output_builders/query_1x_output_builder /controllers/output_builders/query_1x_output_builder

ENTRYPOINT ["/bin/sh"]