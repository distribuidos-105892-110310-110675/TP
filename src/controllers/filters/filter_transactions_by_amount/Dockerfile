FROM python:3.13-slim

COPY src/controllers/filters/filter_transactions_by_amount /filter_transactions_by_amount
COPY src/shared /shared

ENV PYTHONPATH=/

ENTRYPOINT ["/bin/sh"]
