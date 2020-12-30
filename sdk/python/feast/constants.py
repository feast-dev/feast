#
#  Copyright 2019 The Feast Authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
from enum import Enum
from typing import Optional


class AuthProvider(Enum):
    GOOGLE = "google"
    OAUTH = "oauth"


class Option:
    def __init__(self, name, default):
        self._name = name
        self._default = default

    def __get__(self, instance, owner):
        if instance is None:
            return self._name.lower()

        return self._default


class ConfigMeta(type):
    """
    Class factory which customizes ConfigOptions class instantiation.
    Specifically, setting configuration option's name to lowercase of capitalized variable.
    """

    def __new__(cls, name, bases, attrs):
        keys = [
            k for k, v in attrs.items() if not k.startswith("_") and not callable(v)
        ]
        attrs["__config_keys__"] = keys
        attrs.update({k: Option(k, attrs[k]) for k in keys})
        return super().__new__(cls, name, bases, attrs)


#: Default datetime column name for point-in-time join
DATETIME_COLUMN: str = "datetime"

#: Environmental variable to specify Feast configuration file location
FEAST_CONFIG_FILE_ENV: str = "FEAST_CONFIG"

#: Default prefix to Feast environmental variables
CONFIG_FEAST_ENV_VAR_PREFIX: str = "FEAST_"

#: Default directory to Feast configuration file
CONFIG_FILE_DEFAULT_DIRECTORY: str = ".feast"

#: Default Feast configuration file name
CONFIG_FILE_NAME: str = "config"

#: Default section in Feast configuration file to specify options
CONFIG_FILE_SECTION: str = "general"

# Maximum interval(secs) to wait between retries for retry function
MAX_WAIT_INTERVAL: str = "60"


class ConfigOptions(metaclass=ConfigMeta):
    """ Feast Configuration Options """

    #: Feast project namespace to use
    PROJECT: str = "default"

    #: Default Feast Core URL
    CORE_URL: str = "localhost:6565"

    #: Enable or disable TLS/SSL to Feast Core
    CORE_ENABLE_SSL: str = "False"

    #: Enable user authentication to Feast Core
    ENABLE_AUTH: str = "False"

    #: JWT Auth token for user authentication to Feast
    AUTH_TOKEN: Optional[str] = None

    #: Path to certificate(s) to secure connection to Feast Core
    CORE_SERVER_SSL_CERT: str = ""

    #: Default Feast Serving URL
    SERVING_URL: str = "localhost:6566"

    #: Enable or disable TLS/SSL to Feast Serving
    SERVING_ENABLE_SSL: str = "False"

    #: Path to certificate(s) to secure connection to Feast Serving
    SERVING_SERVER_SSL_CERT: str = ""

    #: Default Feast Job Service URL
    JOB_SERVICE_URL: Optional[str] = None

    #: Enable or disable TLS/SSL to Feast Job Service
    JOB_SERVICE_ENABLE_SSL: str = "False"

    #: Path to certificate(s) to secure connection to Feast Job Service
    JOB_SERVICE_SERVER_SSL_CERT: str = ""

    #: Enable or disable control loop for Feast Job Service
    JOB_SERVICE_ENABLE_CONTROL_LOOP: str = "False"

    #: Default connection timeout to Feast Serving, Feast Core, and Feast Job Service (in seconds)
    GRPC_CONNECTION_TIMEOUT: str = "10"

    #: Default gRPC connection timeout when sending an ApplyFeatureTable command to Feast Core (in seconds)
    GRPC_CONNECTION_TIMEOUT_APPLY: str = "600"

    #: Default timeout when running batch ingestion
    BATCH_INGESTION_PRODUCTION_TIMEOUT: str = "120"

    #: Time to wait for historical feature requests before timing out.
    BATCH_FEATURE_REQUEST_WAIT_TIME_SECONDS: str = "600"

    #: Endpoint URL for S3 storage_client
    S3_ENDPOINT_URL: Optional[str] = None

    #: Account name for Azure blob storage_client
    AZURE_BLOB_ACCOUNT_NAME: Optional[str] = None

    #: Account access key for Azure blob storage_client
    AZURE_BLOB_ACCOUNT_ACCESS_KEY: Optional[str] = None

    #: Authentication Provider - Google OpenID/OAuth
    #:
    #: Options: "google" / "oauth"
    AUTH_PROVIDER: str = "google"

    #: Spark Job launcher. The choice of storage is connected to the choice of SPARK_LAUNCHER.
    #:
    #: Options: "standalone", "dataproc", "emr"
    SPARK_LAUNCHER: Optional[str] = None

    #: Feast Spark Job ingestion jobs staging location. The choice of storage is connected to the choice of SPARK_LAUNCHER.
    #:
    #: Eg. gs://some-bucket/output/, s3://some-bucket/output/, file:///data/subfolder/
    SPARK_STAGING_LOCATION: Optional[str] = None

    #: Feast Spark Job ingestion jar file. The choice of storage is connected to the choice of SPARK_LAUNCHER.
    #:
    #: Eg. "dataproc" (http and gs), "emr" (http and s3), "standalone" (http and file)
    SPARK_INGESTION_JAR: str = "https://storage.googleapis.com/feast-jobs/spark/ingestion/feast-ingestion-spark-develop.jar"

    #: Spark resource manager master url
    SPARK_STANDALONE_MASTER: str = "local[*]"

    #: Directory where Spark is installed
    SPARK_HOME: Optional[str] = None

    #: The project id where the materialized view of BigQuerySource is going to be created
    #: by default, use the same project where view is located
    SPARK_BQ_MATERIALIZATION_PROJECT: Optional[str] = None

    #: The dataset id where the materialized view of BigQuerySource is going to be created
    #: by default, use the same dataset where view is located
    SPARK_BQ_MATERIALIZATION_DATASET: Optional[str] = None

    #: Dataproc cluster to run Feast Spark Jobs in
    DATAPROC_CLUSTER_NAME: Optional[str] = None

    #: Project of Dataproc cluster
    DATAPROC_PROJECT: Optional[str] = None

    #: Region of Dataproc cluster
    DATAPROC_REGION: Optional[str] = None

    #: No. of executor instances for Dataproc cluster
    DATAPROC_EXECUTOR_INSTANCES = "2"

    #: No. of executor cores for Dataproc cluster
    DATAPROC_EXECUTOR_CORES = "2"

    #: No. of executor memory for Dataproc cluster
    DATAPROC_EXECUTOR_MEMORY = "2g"

    # namespace to use for Spark jobs launched using k8s spark operator
    SPARK_K8S_NAMESPACE = "default"

    # expect k8s spark operator to be running in the same cluster as Feast
    SPARK_K8S_USE_INCLUSTER_CONFIG = "True"

    # SparkApplication resource template
    SPARK_K8S_JOB_TEMPLATE_PATH = None

    #: File format of historical retrieval features
    HISTORICAL_FEATURE_OUTPUT_FORMAT: str = "parquet"

    #: File location of historical retrieval features
    HISTORICAL_FEATURE_OUTPUT_LOCATION: Optional[str] = None

    #: Default Redis host
    REDIS_HOST: str = "localhost"

    #: Default Redis port
    REDIS_PORT: str = "6379"

    #: Enable or disable TLS/SSL to Redis
    REDIS_SSL: str = "False"

    #: Enable or disable StatsD
    STATSD_ENABLED: str = "False"

    #: Default StatsD port
    STATSD_HOST: Optional[str] = None

    #: Default StatsD port
    STATSD_PORT: Optional[str] = None

    #: Ingestion Job DeadLetter Destination. The choice of storage is connected to the choice of SPARK_LAUNCHER.
    #:
    #: Eg. gs://some-bucket/output/, s3://some-bucket/output/, file:///data/subfolder/
    DEADLETTER_PATH: str = ""

    #: ProtoRegistry Address (currently only Stencil Server is supported as registry)
    #: https://github.com/gojekfarm/stencil
    STENCIL_URL: str = ""

    #: If set to true rows that do not pass custom validation (see feast.contrib.validation)
    #: won't be saved to Online Storage
    INGESTION_DROP_INVALID_ROWS = "False"

    #: EMR cluster to run Feast Spark Jobs in
    EMR_CLUSTER_ID: Optional[str] = None

    #: Region of EMR cluster
    EMR_REGION: Optional[str] = None

    #: Template path of EMR cluster
    EMR_CLUSTER_TEMPLATE_PATH: Optional[str] = None

    #: Log path of EMR cluster
    EMR_LOG_LOCATION: Optional[str] = None

    #: Oauth grant type
    OAUTH_GRANT_TYPE: Optional[str] = None

    #: Oauth client ID
    OAUTH_CLIENT_ID: Optional[str] = None

    #: Oauth client secret
    OAUTH_CLIENT_SECRET: Optional[str] = None

    #: Oauth intended recipients
    OAUTH_AUDIENCE: Optional[str] = None

    #: Oauth token request url
    OAUTH_TOKEN_REQUEST_URL: Optional[str] = None

    def defaults(self):
        return {
            k: getattr(self, k)
            for k in self.__config_keys__
            if getattr(self, k) is not None
        }
