FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/controllers/reducers/count_purchases_by_store_id_and_user_id /controllers/reducers/count_purchases_by_store_id_and_user_id
COPY src/shared /shared
COPY src/middleware /middleware

ENTRYPOINT ["/bin/sh"]