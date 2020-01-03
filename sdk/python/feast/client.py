# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import os
import time
from collections import OrderedDict
from math import ceil
from typing import Dict, Union
from typing import List
from urllib.parse import urlparse

import fastavro
import grpc
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from feast.core.CoreService_pb2 import (
    GetFeastCoreVersionRequest,
    ListFeatureSetsResponse,
    ApplyFeatureSetRequest,
    ListFeatureSetsRequest,
    ApplyFeatureSetResponse,
    GetFeatureSetRequest,
    GetFeatureSetResponse,
)
from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.feature_set import FeatureSet, Entity
from feast.job import Job
from feast.loaders.abstract_producer import get_producer
from feast.loaders.file import export_source_to_staging_location
from feast.loaders.ingest import KAFKA_CHUNK_PRODUCTION_TIMEOUT
from feast.loaders.ingest import get_feature_row_chunks
from feast.serving.ServingService_pb2 import GetFeastServingInfoResponse
from feast.serving.ServingService_pb2 import (
    GetOnlineFeaturesRequest,
    GetBatchFeaturesRequest,
    GetFeastServingInfoRequest,
    GetOnlineFeaturesResponse,
    DatasetSource,
    DataFormat,
    FeatureSetRequest,
    FeastServingType,
)
from feast.serving.ServingService_pb2_grpc import ServingServiceStub

_logger = logging.getLogger(__name__)

GRPC_CONNECTION_TIMEOUT_DEFAULT = 3  # type: int
GRPC_CONNECTION_TIMEOUT_APPLY = 600  # type: int
FEAST_SERVING_URL_ENV_KEY = "FEAST_SERVING_URL"  # type: str
FEAST_CORE_URL_ENV_KEY = "FEAST_CORE_URL"  # type: str
BATCH_FEATURE_REQUEST_WAIT_TIME_SECONDS = 300
CPU_COUNT = os.cpu_count()  # type: int


class Client:
    """
    Feast Client: Used for creating, managing, and retrieving features.
    """

    def __init__(
        self, core_url: str = None, serving_url: str = None, verbose: bool = False
    ):
        """
        The Feast Client should be initialized with at least one service url

        Args:
            core_url: Feast Core URL. Used to manage features
            serving_url: Feast Serving URL. Used to retrieve features
            verbose: Enable verbose logging
        """
        self._core_url = core_url
        self._serving_url = serving_url
        self._verbose = verbose
        self.__core_channel: grpc.Channel = None
        self.__serving_channel: grpc.Channel = None
        self._core_service_stub: CoreServiceStub = None
        self._serving_service_stub: ServingServiceStub = None

    @property
    def core_url(self) -> str:
        """
        Retrieve Feast Core URL
        """

        if self._core_url is not None:
            return self._core_url
        if os.getenv(FEAST_CORE_URL_ENV_KEY) is not None:
            return os.getenv(FEAST_CORE_URL_ENV_KEY)
        return ""

    @core_url.setter
    def core_url(self, value: str):
        """
        Set the Feast Core URL

        Returns:
            Feast Core URL string
        """
        self._core_url = value

    @property
    def serving_url(self) -> str:
        """
        Retrieve Serving Core URL
        """
        if self._serving_url is not None:
            return self._serving_url
        if os.getenv(FEAST_SERVING_URL_ENV_KEY) is not None:
            return os.getenv(FEAST_SERVING_URL_ENV_KEY)
        return ""

    @serving_url.setter
    def serving_url(self, value: str):
        """
        Set the Feast Serving URL

        Returns:
            Feast Serving URL string
        """
        self._serving_url = value

    def version(self):
        """
        Returns version information from Feast Core and Feast Serving
        """
        result = {}

        if self.serving_url:
            self._connect_serving()
            serving_version = self._serving_service_stub.GetFeastServingInfo(
                GetFeastServingInfoRequest(), timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
            ).version
            result["serving"] = {"url": self.serving_url, "version": serving_version}

        if self.core_url:
            self._connect_core()
            core_version = self._core_service_stub.GetFeastCoreVersion(
                GetFeastCoreVersionRequest(), timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
            ).version
            result["core"] = {"url": self.core_url, "version": core_version}

        return result

    def _connect_core(self, skip_if_connected: bool = True):
        """
        Connect to Core API

        Args:
            skip_if_connected: Do not attempt to connect if already connected
        """
        if skip_if_connected and self._core_service_stub:
            return

        if not self.core_url:
            raise ValueError("Please set Feast Core URL.")

        if self.__core_channel is None:
            self.__core_channel = grpc.insecure_channel(self.core_url)

        try:
            grpc.channel_ready_future(self.__core_channel).result(
                timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
            )
        except grpc.FutureTimeoutError:
            raise ConnectionError(
                f"Connection timed out while attempting to connect to Feast "
                f"Core gRPC server {self.core_url} "
            )
        else:
            self._core_service_stub = CoreServiceStub(self.__core_channel)

    def _connect_serving(self, skip_if_connected=True):
        """
        Connect to Serving API

        Args:
            skip_if_connected: Do not attempt to connect if already connected
        """

        if skip_if_connected and self._serving_service_stub:
            return

        if not self.serving_url:
            raise ValueError("Please set Feast Serving URL.")

        if self.__serving_channel is None:
            self.__serving_channel = grpc.insecure_channel(self.serving_url)

        try:
            grpc.channel_ready_future(self.__serving_channel).result(
                timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
            )
        except grpc.FutureTimeoutError:
            raise ConnectionError(
                f"Connection timed out while attempting to connect to Feast "
                f"Serving gRPC server {self.serving_url} "
            )
        else:
            self._serving_service_stub = ServingServiceStub(self.__serving_channel)

    def apply(self, feature_sets: Union[List[FeatureSet], FeatureSet]):
        """
        Idempotently registers feature set(s) with Feast Core. Either a single
        feature set or a list can be provided.

        Args:
            feature_sets: List of feature sets that will be registered
        """
        if not isinstance(feature_sets, list):
            feature_sets = [feature_sets]
        for feature_set in feature_sets:
            if isinstance(feature_set, FeatureSet):
                self._apply_feature_set(feature_set)
                continue
            raise ValueError(
                f"Could not determine feature set type to apply {feature_set}"
            )

    def _apply_feature_set(self, feature_set: FeatureSet):
        """
        Registers a single feature set with Feast

        Args:
            feature_set: Feature set that will be registered
        """
        self._connect_core()
        feature_set._client = self

        feature_set.is_valid()

        # Convert the feature set to a request and send to Feast Core
        apply_fs_response = self._core_service_stub.ApplyFeatureSet(
            ApplyFeatureSetRequest(feature_set=feature_set.to_proto()),
            timeout=GRPC_CONNECTION_TIMEOUT_APPLY,
        )  # type: ApplyFeatureSetResponse

        # Extract the returned feature set
        applied_fs = FeatureSet.from_proto(apply_fs_response.feature_set)

        # If the feature set has changed, update the local copy
        if apply_fs_response.status == ApplyFeatureSetResponse.Status.CREATED:
            print(
                f'Feature set updated/created: "{applied_fs.name}:{applied_fs.version}"'
            )

        # If no change has been applied, do nothing
        if apply_fs_response.status == ApplyFeatureSetResponse.Status.NO_CHANGE:
            print(f"No change detected or applied: {feature_set.name}")

        # Deep copy from the returned feature set to the local feature set
        feature_set._update_from_feature_set(applied_fs)

    def list_feature_sets(self) -> List[FeatureSet]:
        """
        Retrieve a list of feature sets from Feast Core

        Returns:
            List of feature sets
        """
        self._connect_core()

        # Get latest feature sets from Feast Core
        feature_set_protos = self._core_service_stub.ListFeatureSets(
            ListFeatureSetsRequest()
        )  # type: ListFeatureSetsResponse

        # Extract feature sets and return
        feature_sets = []
        for feature_set_proto in feature_set_protos.feature_sets:
            feature_set = FeatureSet.from_proto(feature_set_proto)
            feature_set._client = self
            feature_sets.append(feature_set)
        return feature_sets

    def get_feature_set(
        self, name: str, version: int = None
    ) -> Union[FeatureSet, None]:
        """
        Retrieves a feature set. If no version is specified then the latest
        version will be returned.

        Args:
            name: Name of feature set
            version: Version of feature set

        Returns:
            Returns either the specified feature set, or raises an exception if
            none is found
        """
        self._connect_core()

        if version is None:
            version = 0
        get_feature_set_response = self._core_service_stub.GetFeatureSet(
            GetFeatureSetRequest(name=name.strip(), version=int(version))
        )  # type: GetFeatureSetResponse
        return FeatureSet.from_proto(get_feature_set_response.feature_set)

    def list_entities(self) -> Dict[str, Entity]:
        """
        Returns a dictionary of entities across all feature sets

        Returns:
            Dictionary of entities, indexed by name
        """
        entities_dict = OrderedDict()
        for fs in self.list_feature_sets():
            for entity in fs.entities:
                entities_dict[entity.name] = entity
        return entities_dict

    def get_batch_features(
        self, feature_ids: List[str], entity_rows: Union[pd.DataFrame, str]
    ) -> Job:
        """
        Retrieves historical features from a Feast Serving deployment.

        Args:
            feature_ids (List[str]):
                List of feature ids that will be returned for each entity.
                Each feature id should have the following format
                "feature_set_name:version:feature_name".

            entity_rows (Union[pd.DataFrame, str]):
                Either:
                Pandas dataframe containing entities and a 'datetime' column.
                Each entity in a feature set must be present as a column in this
                dataframe. The datetime column must contain timestamps in
                datetime64 format.

                Or:
                A file path in AVRO format representing the entity rows.

        Returns:
            feast.job.Job:
                Returns a job object that can be used to monitor retrieval
                progress asynchronously, and can be used to materialize the
                results.

        Examples:
            >>> from feast import Client
            >>> from datetime import datetime
            >>>
            >>> feast_client = Client(core_url="localhost:6565", serving_url="localhost:6566")
            >>> feature_ids = ["customer:1:bookings_7d"]
            >>> entity_rows = pd.DataFrame(
            >>>         {
            >>>            "datetime": [pd.datetime.now() for _ in range(3)],
            >>>            "customer": [1001, 1002, 1003],
            >>>         }
            >>>     )
            >>> feature_retrieval_job = feast_client.get_batch_features(feature_ids, entity_rows)
            >>> df = feature_retrieval_job.to_dataframe()
            >>> print(df)
        """

        self._connect_serving()

        fs_request = _build_feature_set_request(feature_ids)

        # Retrieve serving information to determine store type and
        # staging location
        serving_info = self._serving_service_stub.GetFeastServingInfo(
            GetFeastServingInfoRequest(),
            timeout=GRPC_CONNECTION_TIMEOUT_DEFAULT
        )  # type: GetFeastServingInfoResponse

        if serving_info.type != FeastServingType.FEAST_SERVING_TYPE_BATCH:
            raise Exception(
                f'You are connected to a store "{self._serving_url}" which '
                f"does not support batch retrieval "
            )

        if isinstance(entity_rows, pd.DataFrame):
            # Pandas DataFrame detected
            # Validate entity rows to based on entities in Feast Core
            self._validate_dataframe_for_batch_retrieval(
                entity_rows=entity_rows,
                feature_sets_request=fs_request
            )

            # Remove timezone from datetime column
            if isinstance(
                    entity_rows["datetime"].dtype,
                    pd.core.dtypes.dtypes.DatetimeTZDtype
            ):
                entity_rows["datetime"] = pd.DatetimeIndex(
                    entity_rows["datetime"]
                ).tz_localize(None)
        elif isinstance(entity_rows, str):
            # String based source
            if entity_rows.endswith((".avro", "*")):
                # Validate Avro entity rows to based on entities in Feast Core
                self._validate_avro_for_batch_retrieval(
                    source=entity_rows,
                    feature_sets_request=fs_request
                )
            else:
                raise Exception(
                    f"Only .avro and wildcard paths are accepted as entity_rows"
                )
        else:
            raise Exception(f"Only pandas.DataFrame and str types are allowed"
                            f" as entity_rows, but got {type(entity_rows)}.")

        # Export and upload entity row DataFrame to staging location
        # provided by Feast
        staged_files = export_source_to_staging_location(
            entity_rows, serving_info.job_staging_location
        )  # type: List[str]

        request = GetBatchFeaturesRequest(
            feature_sets=fs_request,
            dataset_source=DatasetSource(
                file_source=DatasetSource.FileSource(
                    file_uris=staged_files,
                    data_format=DataFormat.DATA_FORMAT_AVRO
                )
            ),
        )

        # Retrieve Feast Job object to manage life cycle of retrieval
        response = self._serving_service_stub.GetBatchFeatures(request)
        return Job(response.job, self._serving_service_stub)

    def _validate_dataframe_for_batch_retrieval(
        self, entity_rows: pd.DataFrame, feature_sets_request
    ):
        """
        Validate whether an the entity rows in a DataFrame contains the correct
        information for batch retrieval.

        Datetime column must be present in the DataFrame.

        Args:
            entity_rows (pd.DataFrame):
                Pandas DataFrame containing entities and datetime column. Each
                entity in a feature set must be present as a column in this
                DataFrame.

            feature_sets_request:
                Feature sets that will be requested.
        """

        self._validate_columns(
            columns=entity_rows.columns,
            feature_sets_request=feature_sets_request,
            datetime_field="datetime"
        )

    def _validate_avro_for_batch_retrieval(
            self, source: str, feature_sets_request
    ):
        """
        Validate whether the entity rows in an Avro source file contains the
        correct information for batch retrieval.

        Only gs:// and local files (file://) uri schemes are allowed.

        Avro file must have a column named "event_timestamp".

        No checks will be done if a GCS path is provided.

        Args:
            source (str):
                File path to Avro.

            feature_sets_request:
                Feature sets that will be requested.
        """
        p = urlparse(source)

        if p.scheme == "gs":
            # GCS path provided (Risk is delegated to user)
            # No validation if GCS path is provided
            return
        elif p.scheme == "file" or not p.scheme:
            # Local file (file://) provided
            file_path = os.path.abspath(os.path.join(p.netloc, p.path))
        else:
            raise Exception(f"Unsupported uri scheme provided {p.scheme}, only "
                            f"local files (file://), and gs:// schemes are "
                            f"allowed")

        with open(file_path, "rb") as f:
            reader = fastavro.reader(f)
            schema = json.loads(reader.metadata["avro.schema"])
            columns = [x["name"] for x in schema["fields"]]
            self._validate_columns(
                columns=columns,
                feature_sets_request=feature_sets_request,
                datetime_field="event_timestamp"
            )

    def _validate_columns(
            self, columns: List[str],
            feature_sets_request,
            datetime_field: str
    ) -> None:
        """
        Check if the required column contains the correct values for batch
        retrieval.

        Args:
            columns (List[str]):
                List of columns to validate against feature_sets_request.

            feature_sets_request ():
                Feature sets that will be requested.

            datetime_field (str):
                Name of the datetime field that must be enforced and present as
                a column in the data source.

        Returns:
            None:
                None
        """
        # Ensure datetime column exists
        if datetime_field not in columns:
            raise ValueError(
                f'Entity rows does not contain "{datetime_field}" column in '
                f'columns {columns}'
            )

        # Validate Avro columns based on feature set entities
        for feature_set in feature_sets_request:
            fs = self.get_feature_set(
                name=feature_set.name, version=feature_set.version
            )
            if fs is None:
                raise ValueError(
                    f'Feature set "{feature_set.name}:{feature_set.version}" '
                    f"could not be found"
                )
            for entity_type in fs.entities:
                if entity_type.name not in columns:
                    raise ValueError(
                        f'Input does not contain entity'
                        f' "{entity_type.name}" column in columns "{columns}"'
                    )

    def get_online_features(
        self,
        feature_ids: List[str],
        entity_rows: List[GetOnlineFeaturesRequest.EntityRow],
    ) -> GetOnlineFeaturesResponse:
        """
        Retrieves the latest online feature data from Feast Serving

        Args:
            feature_ids: List of feature Ids in the following format
                [feature_set_name]:[version]:[feature_name]
                example:
                    ["feature_set_1:6:my_feature_1",
                    "feature_set_1:6:my_feature_2",]
            entity_rows: List of GetFeaturesRequest.EntityRow where each row
                contains entities. Timestamp should not be set for online
                retrieval. All entity types within a feature

        Returns:
            Returns a list of maps where each item in the list contains the
            latest feature values for the provided entities
        """

        self._connect_serving()

        return self._serving_service_stub.GetOnlineFeatures(
            GetOnlineFeaturesRequest(
                feature_sets=_build_feature_set_request(feature_ids),
                entity_rows=entity_rows,
            )
        )  # type: GetOnlineFeaturesResponse

    def ingest(
            self,
            feature_set: Union[str, FeatureSet],
            source: Union[pd.DataFrame, str],
            chunk_size: int = 10000,
            version: int = None,
            force_update: bool = False,
            max_workers: int = max(CPU_COUNT - 1, 1),
            disable_progress_bar: bool = False,
            timeout: int = KAFKA_CHUNK_PRODUCTION_TIMEOUT
    ) -> None:
        """
        Loads feature data into Feast for a specific feature set.

        Args:
            feature_set (typing.Union[str, FeatureSet]):
                Feature set object or the string name of the feature set
                (without a version).

            source (typing.Union[pd.DataFrame, str]):
                Either a file path or Pandas Dataframe to ingest into Feast
                Files that are currently supported:
                    * parquet
                    * csv
                    * json

            chunk_size (int):
                Amount of rows to load and ingest at a time.

            version (int):
                Feature set version.

            force_update (bool):
                Automatically update feature set based on source data prior to
                ingesting. This will also register changes to Feast.

            max_workers (int):
                Number of worker processes to use to encode values.

            disable_progress_bar (bool):
                Disable printing of progress statistics.

            timeout (int):
                Timeout in seconds to wait for completion.

        Returns:
            None:
                None
        """

        if isinstance(feature_set, FeatureSet):
            name = feature_set.name
            if version is None:
                version = feature_set.version
        elif isinstance(feature_set, str):
            name = feature_set
        else:
            raise Exception(f"Feature set name must be provided")

        # Read table and get row count
        tmp_table_name = _read_table_from_source(
            source, chunk_size, max_workers
        )

        pq_file = pq.ParquetFile(tmp_table_name)

        row_count = pq_file.metadata.num_rows

        # Update the feature set based on PyArrow table of first row group
        if force_update:
            feature_set.infer_fields_from_pa(
                table=pq_file.read_row_group(0),
                discard_unused_fields=True,
                replace_existing_features=True
            )
            self.apply(feature_set)

        feature_set = self.get_feature_set(name, version)

        try:
            # Kafka configs
            brokers = feature_set.get_kafka_source_brokers()
            topic = feature_set.get_kafka_source_topic()
            producer = get_producer(brokers, row_count, disable_progress_bar)

            # Loop optimization declarations
            produce = producer.produce
            flush = producer.flush

            # Transform and push data to Kafka
            if feature_set.source.source_type == "Kafka":
                for chunk in get_feature_row_chunks(
                        file=tmp_table_name,
                        row_groups=list(range(pq_file.num_row_groups)),
                        fs=feature_set,
                        max_workers=max_workers):

                    # Push FeatureRow one chunk at a time to kafka
                    for serialized_row in chunk:
                        produce(topic=topic, value=serialized_row)

                    # Force a flush after each chunk
                    flush(timeout=timeout)

                    # Remove chunk from memory
                    del chunk

            else:
                raise Exception(
                    f"Could not determine source type for feature set "
                    f'"{feature_set.name}" with source type '
                    f'"{feature_set.source.source_type}"'
                )

            # Print ingestion statistics
            producer.print_results()
        finally:
            # Remove parquet file(s) that were created earlier
            print("Removing temporary file(s)...")
            os.remove(tmp_table_name)

        return None


def _build_feature_set_request(
        feature_ids: List[str]
) -> List[FeatureSetRequest]:
    """
    Builds a list of FeatureSet objects from feature set ids in order to
    retrieve feature data from Feast Serving

    Args:
        feature_ids: List of feature ids
            ("feature_set_name:version:feature_name")
    """
    feature_set_request = dict()  # type: Dict[str, FeatureSetRequest]
    for feature_id in feature_ids:
        fid_parts = feature_id.split(":")
        if len(fid_parts) == 3:
            feature_set, version, feature = fid_parts
        else:
            raise ValueError(
                f"Could not parse feature id ${feature_id}, needs 2 colons"
            )

        if feature_set not in feature_set_request:
            feature_set_request[feature_set] = FeatureSetRequest(
                name=feature_set, version=int(version)
            )
        feature_set_request[feature_set].feature_names.append(feature)
    return list(feature_set_request.values())


def _read_table_from_source(
        source: Union[pd.DataFrame, str],
        chunk_size: int,
        max_workers: int
) -> str:
    """
    Infers a data source type (path or Pandas DataFrame) and reads it in as
    a PyArrow Table.

    The PyArrow Table that is read will be written to a parquet file with row
    group size determined by the minimum of:
        * (table.num_rows / max_workers)
        * chunk_size

    The parquet file that is created will be passed as file path to the
    multiprocessing pool workers.

    Args:
        source (Union[pd.DataFrame, str]):
            Either a string path or Pandas DataFrame.

        chunk_size (int):
            Number of worker processes to use to encode values.

        max_workers (int):
            Amount of rows to load and ingest at a time.

    Returns:
        str: Path to parquet file that was created.
    """

    # Pandas DataFrame detected
    if isinstance(source, pd.DataFrame):
        table = pa.Table.from_pandas(df=source)

    # Inferring a string path
    elif isinstance(source, str):
        file_path = source
        filename, file_ext = os.path.splitext(file_path)

        if ".csv" in file_ext:
            from pyarrow import csv

            table = csv.read_csv(filename)
        elif ".json" in file_ext:
            from pyarrow import json

            table = json.read_json(filename)
        else:
            table = pq.read_table(file_path)
    else:
        raise ValueError(
            f"Unknown data source provided for ingestion: {source}")

    # Ensure that PyArrow table is initialised
    assert isinstance(table, pa.lib.Table)

    # Write table as parquet file with a specified row_group_size
    tmp_table_name = f"{int(time.time())}.parquet"
    row_group_size = min(ceil(table.num_rows / max_workers), chunk_size)
    pq.write_table(table=table, where=tmp_table_name,
                   row_group_size=row_group_size)

    # Remove table from memory
    del table

    return tmp_table_name
