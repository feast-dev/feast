import inspect
import logging
import multiprocessing
import os
import shutil
import uuid
import warnings
from datetime import datetime
from os.path import expanduser, join
from typing import Any, Dict, List, Optional, Union

import grpc
import pandas as pd
import requests
from google.protobuf.json_format import MessageToJson, ParseDict

from feast.config import Config
from feast.constants import ConfigOptions as opt
from feast.core.CoreService_pb2 import (
    ApplyEntityRequest,
    ApplyEntityResponse,
    ApplyFeatureTableRequest,
    ApplyFeatureTableResponse,
    ArchiveProjectRequest,
    ArchiveProjectResponse,
    CreateProjectRequest,
    CreateProjectResponse,
    DeleteFeatureTableRequest,
    GetEntityRequest,
    GetEntityResponse,
    GetFeastCoreVersionRequest,
    GetFeatureTableRequest,
    GetFeatureTableResponse,
    ListEntitiesRequest,
    ListEntitiesResponse,
    ListFeaturesRequest,
    ListFeaturesResponse,
    ListFeatureTablesRequest,
    ListFeatureTablesResponse,
    ListProjectsRequest,
    ListProjectsResponse,
)
from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.data_format import ParquetFormat
from feast.data_source import BigQuerySource, FileSource
from feast.entity import Entity
from feast.feature import Feature, FeatureRef, _build_feature_references
from feast.feature_table import FeatureTable
import json


def grpc_rest_proxy(func, return_msg_type=None):
    def wrapper(*args, **kwargs):
        base_url = args[0].url
        service_name = args[0].service_name

        # Turn gRPC requests into dict
        request_grpc = args[1]
        request_json = MessageToJson(request_grpc)
        url = os.path.join(base_url, service_name, func.__name__)

        # Send request to REST endpoint
        response = requests.post(url, data=request_json)

        # Parse return dict into return message type
        signature = inspect.signature(func)
        return_type = signature.return_annotation

        response_json = json.loads(response.text)
        return_msg = return_type()
        ParseDict(response_json, return_msg)
        return return_msg

    return wrapper


class CoreServiceRESTStub(object):
    def __init__(self, core_url) -> None:
        super().__init__()
        self.url = core_url
        self.service_name = "feast.core.CoreService"

    @grpc_rest_proxy
    def ListProjects(self, req: ListProjectsRequest, *args,
                     **kwargs) -> ListProjectsResponse:
        pass
