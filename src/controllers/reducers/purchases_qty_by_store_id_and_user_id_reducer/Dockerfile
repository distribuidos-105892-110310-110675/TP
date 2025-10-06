FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/shared /shared
COPY src/middleware /middleware
COPY src/controllers/controller.py /controllers/controller.py

COPY src/controllers/reducers/reducer.py /controllers/reducers/reducer.py
COPY src/controllers/reducers/purchases_qty_by_store_id_and_user_id_reducer /controllers/reducers/purchases_qty_by_store_id_and_user_id_reducer

ENTRYPOINT ["/bin/sh"]