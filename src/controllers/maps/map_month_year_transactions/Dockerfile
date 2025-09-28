FROM python:3.13-slim

COPY src/controllers/maps/map_month_year_transactions /map_month_year_transactions
COPY src/shared /shared

ENV PYTHONPATH=/

ENTRYPOINT ["/bin/sh"]
