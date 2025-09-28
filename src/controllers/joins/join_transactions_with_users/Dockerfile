FROM python:3.13-slim

COPY src/controllers/joins/join_transactions_with_users /join_transactions_with_users
COPY src/shared /shared

ENV PYTHONPATH=/

ENTRYPOINT ["/bin/sh"]
