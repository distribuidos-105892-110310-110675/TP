FROM python:3.13-slim

COPY src/controllers/filters/filter_transactions_by_year /filter_transactions_by_year
COPY src/shared /shared

ENV PYTHONPATH=/

ENTRYPOINT ["/bin/sh"]
