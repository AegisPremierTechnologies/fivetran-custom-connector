"""Data transformations for Springboard MongoDB documents.

Pure input/output: converts BSON documents to Fivetran-compatible row dicts.
No side effects, no database access, no Fivetran operations.
"""

import json
from datetime import datetime
from typing import Any, Optional

from bson import ObjectId


class BSONEncoder(json.JSONEncoder):
    """JSON encoder that handles common BSON types from MongoDB."""

    def default(self, o: Any) -> Any:
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, bytes):
            return o.hex()
        return super().default(o)


def flatten_document(doc: dict) -> dict:
    """Convert a MongoDB document to a flat two-column row for Fivetran.

    The _id is extracted as a STRING primary key. The entire document
    (including _id) is serialized as a JSON string in raw_document.

    Args:
        doc: Raw MongoDB document dict.

    Returns:
        { "_id": str, "raw_document": str }
    """
    doc_id = doc.get("_id")
    return {
        "_id": str(doc_id) if doc_id is not None else None,
        "raw_document": json.dumps(doc, cls=BSONEncoder, default=str),
    }


def extract_field_names(docs: list[dict]) -> list[str]:
    """Collect the union of all top-level keys across a batch of documents.

    Useful for logging document shapes during exploration.

    Args:
        docs: List of MongoDB document dicts.

    Returns:
        Sorted list of unique top-level field names.
    """
    keys: set[str] = set()
    for doc in docs:
        keys.update(doc.keys())
    return sorted(keys)
