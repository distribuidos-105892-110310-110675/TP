FROM python:3.13-slim

WORKDIR /app

COPY src/controllers/filters/filter_by_hour/ .
COPY .data/transactions/ /app/data/

CMD ["python", "main.py"]