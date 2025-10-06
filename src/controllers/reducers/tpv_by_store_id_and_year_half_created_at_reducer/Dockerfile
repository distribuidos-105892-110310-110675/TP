FROM python:3.13-slim

RUN pip install --upgrade pip && pip3 install pika

COPY src/shared /shared
COPY src/middleware /middleware
COPY src/controllers/controller.py /controllers/controller.py

COPY src/controllers/reducers/reducer.py /controllers/reducers/reducer.py
COPY src/controllers/reducers/tpv_by_store_id_and_year_half_created_at_reducer /controllers/reducers/tpv_by_store_id_and_year_half_created_at_reducer

ENTRYPOINT ["/bin/sh"]