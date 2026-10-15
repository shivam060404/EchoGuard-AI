"""Production persistence wiring for the local MongoDB deployment."""
from contextlib import contextmanager
import os

from backend.knowledge.retriever import MongoHybridRetriever
from .graph import build_graph
from .provider import MockPlanningProvider
from .service import WorkflowService
from .store import MongoWorkflowStore


@contextmanager
def runtime(database_name=None):
    from pymongo import MongoClient
    from langgraph.checkpoint.mongodb import MongoDBSaver

    database_name = database_name or os.getenv("WORKFLOW_DATABASE", "echoguard")
    if not database_name or any(char in database_name for char in '/\\. "$*<>:|?'):
        raise ValueError("Invalid workflow database name")
    with MongoClient(os.getenv("MONGO_URI", "mongodb://mongo:27017"), tz_aware=True,
                     serverSelectionTimeoutMS=5000, connectTimeoutMS=5000,
                     socketTimeoutMS=10000) as client:
        client.admin.command("ping")
        database = client[database_name]
        saver = MongoDBSaver(client, db_name=database_name,
                             checkpoint_collection_name="workflow_checkpoints_v1",
                             writes_collection_name="workflow_checkpoint_writes_v1")
        retriever = MongoHybridRetriever(database)
        store = MongoWorkflowStore(database)
        graph = build_graph(MockPlanningProvider(), retriever, saver)
        yield WorkflowService(graph, store, retriever), database
