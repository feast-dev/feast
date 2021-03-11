import inspect
import json
import os

import requests
from google.protobuf.json_format import MessageToJson, ParseDict


def rest_transport(func):
    def wrapper(*args, **kwargs):
        base_url = args[0]._url
        service_name = args[0]._service_name

        # Turn gRPC requests into json.
        request_grpc = args[1]
        request_json = MessageToJson(request_grpc)
        url = os.path.join(base_url, service_name, func.__name__)

        # Send request to REST endpoint
        response = requests.post(url, data=request_json)

        if response.status_code != 200:
            raise RuntimeError(
                "Response code is not 200 due to: {}".format(response.text)
            )
        response_json = response.json()

        # Parse return dict into return message type
        signature = inspect.signature(func)
        return_msg_type = signature.return_annotation
        return_msg = return_msg_type()
        ParseDict(response_json, return_msg)

        return return_msg

    return wrapper
