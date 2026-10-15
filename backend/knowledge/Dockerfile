FROM python:3.12.15-slim
WORKDIR /app
COPY requirements.txt /app/backend/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/backend/requirements.txt \
    && python -c "import pymongo"
COPY __init__.py /app/backend/__init__.py
COPY knowledge /app/backend/knowledge
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
USER 10001:10001
CMD ["python", "-m", "backend.knowledge.evaluate"]
