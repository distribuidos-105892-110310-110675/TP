FROM python:3.13-slim

COPY src/controllers/joins/join_users_with_stores /join_users_with_stores
COPY src/shared /shared

ENV PYTHONPATH=/

ENTRYPOINT ["/bin/sh"]
