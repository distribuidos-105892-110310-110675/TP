FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/shared /shared
COPY src/middleware /middleware

COPY src/controllers/shared /controllers/shared
COPY src/controllers/joiners/shared /controllers/joiners/shared
COPY src/controllers/joiners/transactions_with_users_joiner/ /controllers/joiners/transactions_with_users_joiner/

ENTRYPOINT ["/bin/sh"]