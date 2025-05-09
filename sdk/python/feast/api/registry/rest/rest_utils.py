from typing import Dict, List

from fastapi import HTTPException, Query
from google.protobuf.json_format import MessageToDict

from feast.errors import FeastObjectNotFoundException


def grpc_call(handler_fn, request):
    """
    Wrapper to invoke gRPC method with context=None and handle common errors.
    """
    try:
        response = handler_fn(request, context=None)
        return MessageToDict(response)
    except FeastObjectNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error")


def parse_tags(tags: List[str] = Query(default=[])) -> Dict[str, str]:
    """
    Parses query strings like ?tags=key1:value1&tags=key2:value2 into a dict.
    """
    parsed_tags = {}
    for tag in tags:
        if ":" not in tag:
            continue
        key, value = tag.split(":", 1)
        parsed_tags[key] = value
    return parsed_tags
