FROM python:3.12.15-slim
WORKDIR /app
COPY requirements.txt /app/backend/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/backend/requirements.txt \
    && python -c "from langgraph.checkpoint.mongodb import MongoDBSaver"
COPY . /app/backend
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1 LANGCHAIN_TRACING_V2=false
USER 10001:10001
ENTRYPOINT ["python", "-m", "backend.workflow.cli"]
