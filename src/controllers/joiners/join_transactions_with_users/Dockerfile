FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/joiners/join_transactions_with_users /controllers/joiners/join_transactions_with_users
COPY src/shared /shared
COPY src/middleware /middleware

ENTRYPOINT ["/bin/sh"]