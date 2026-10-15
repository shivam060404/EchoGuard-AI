FROM python:3.12.15-slim
WORKDIR /app
COPY requirements.txt /app/backend/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/backend/requirements.txt \
    && python -c "import confluent_kafka, pymongo"
COPY . /app/backend
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
USER 10001:10001
CMD ["python", "-m", "backend.worker.consumer"]
