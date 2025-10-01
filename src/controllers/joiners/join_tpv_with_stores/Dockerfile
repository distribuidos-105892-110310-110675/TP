FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/joiners/join_tpv_with_stores /controllers/joiners/join_tpv_with_stores
COPY src/shared /shared
COPY src/middleware /middleware

ENV PYTHONPATH=/

ENTRYPOINT ["/bin/sh"]