FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/shared /shared
COPY src/middleware /middleware

COPY src/controllers/shared /controllers/shared
COPY src/controllers/cleaners/shared /controllers/cleaners/shared
COPY src/controllers/cleaners/users_cleaner /controllers/cleaners/users_cleaner

ENTRYPOINT ["/bin/sh"]