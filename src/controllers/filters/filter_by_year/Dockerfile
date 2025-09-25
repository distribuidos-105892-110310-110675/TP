FROM python:3.13-slim

WORKDIR /app

COPY src/controllers/filters/filter_by_year/ .
COPY .data/transaction_items/ /app/data/

CMD ["python", "main.py"]