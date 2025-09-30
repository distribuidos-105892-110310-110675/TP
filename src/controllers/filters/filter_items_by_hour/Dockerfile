FROM python:3.13-slim

COPY src/controllers/filters/filter_items_by_hour /filter_items_by_hour
COPY src/shared /shared

ENV PYTHONPATH=/

ENTRYPOINT ["/bin/sh"]
