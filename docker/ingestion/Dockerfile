FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /workspace
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY ingestion ./ingestion
COPY processing ./processing
COPY warehouse ./warehouse
COPY config ./config

CMD ["python", "ingestion/ingest_raw.py"]

