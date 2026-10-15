from fastapi import HTTPException
from backend.workflow.runtime import runtime


def console():
    # The bounded connection is scoped to each request; health endpoints do not open it.
    from pymongo.errors import PyMongoError
    try:
        with runtime() as value:
            yield value
    except PyMongoError:
        raise HTTPException(503, "Storage unavailable; retry with the same request key") from None
