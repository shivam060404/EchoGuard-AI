FROM python:3.12.15-slim
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY contracts /app/backend/contracts
COPY ingest /app/backend/ingest
COPY __init__.py /app/backend/__init__.py
ENV PYTHONPATH=/app
ENTRYPOINT ["python", "-m", "backend.ingest.cli"]

