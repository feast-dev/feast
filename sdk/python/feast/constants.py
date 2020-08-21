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


class AuthProvider(Enum):
    GOOGLE = "google"
    OAUTH = "oauth"


DATETIME_COLUMN = "datetime"

# Environmental variable to specify Feast configuration file location
FEAST_CONFIG_FILE_ENV_KEY = "FEAST_CONFIG"

# Default prefix to Feast environmental variables
CONFIG_FEAST_ENV_VAR_PREFIX = "FEAST_"

# Default directory to Feast configuration file
CONFIG_FILE_DEFAULT_DIRECTORY = ".feast"

# Default Feast configuration file name
CONFIG_FILE_NAME = "config"

# Default section in Feast configuration file to specify options
CONFIG_FILE_SECTION = "general"

# Feast Configuration Options
CONFIG_PROJECT_KEY = "project"
CONFIG_CORE_URL_KEY = "core_url"
CONFIG_CORE_ENABLE_SSL_KEY = "core_enable_ssl"
CONFIG_ENABLE_AUTH_KEY = "enable_auth"
CONFIG_ENABLE_AUTH_TOKEN_KEY = "auth_token"
CONFIG_CORE_SERVER_SSL_CERT_KEY = "core_server_ssl_cert"
CONFIG_JOB_CONTROLLER_SERVER_KEY = "jobcontroller_url"
CONFIG_SERVING_URL_KEY = "serving_url"
CONFIG_SERVING_ENABLE_SSL_KEY = "serving_enable_ssl"
CONFIG_SERVING_SERVER_SSL_CERT_KEY = "serving_server_ssl_cert"
CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY = "grpc_connection_timeout_default"
CONFIG_GRPC_CONNECTION_TIMEOUT_APPLY_KEY = "grpc_connection_timeout_apply"
CONFIG_BATCH_FEATURE_REQUEST_WAIT_TIME_SECONDS_KEY = (
    "batch_feature_request_wait_time_seconds"
)
CONFIG_OAUTH_GRANT_TYPE_KEY = "oauth_grant_type"
CONFIG_OAUTH_CLIENT_ID_KEY = "oauth_client_id"
CONFIG_OAUTH_CLIENT_SECRET_KEY = "oauth_client_secret"
CONFIG_OAUTH_AUDIENCE_KEY = "oauth_audience"
CONFIG_OAUTH_TOKEN_REQUEST_URL_KEY = "oauth_token_request_url"
CONFIG_AUTH_PROVIDER = "auth_provider"

CONFIG_TIMEOUT_KEY = "timeout"
CONFIG_MAX_WAIT_INTERVAL_KEY = "max_wait_interval"

# Configuration option default values
FEAST_DEFAULT_OPTIONS = {
    # Default Feast project to use
    CONFIG_PROJECT_KEY: "default",
    # Default Feast Core URL
    CONFIG_CORE_URL_KEY: "localhost:6565",
    # Enable or disable TLS/SSL to Feast Core
    CONFIG_CORE_ENABLE_SSL_KEY: "False",
    # Enable user authentication to Feast Core
    CONFIG_ENABLE_AUTH_KEY: "False",
    # Path to certificate(s) to secure connection to Feast Core
    CONFIG_CORE_SERVER_SSL_CERT_KEY: "",
    # Default Feast Job Controller URL
    CONFIG_JOB_CONTROLLER_SERVER_KEY: "localhost:6570",
    # Default Feast Serving URL
    CONFIG_SERVING_URL_KEY: "localhost:6565",
    # Enable or disable TLS/SSL to Feast Serving
    CONFIG_SERVING_ENABLE_SSL_KEY: "False",
    # Path to certificate(s) to secure connection to Feast Serving
    CONFIG_SERVING_SERVER_SSL_CERT_KEY: "",
    # Default connection timeout to Feast Serving and Feast Core (in seconds)
    CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY: "3",
    # Default gRPC connection timeout when sending an ApplyFeatureSet command to
    # Feast Core (in seconds)
    CONFIG_GRPC_CONNECTION_TIMEOUT_APPLY_KEY: "600",
    # Time to wait for batch feature requests before timing out.
    CONFIG_BATCH_FEATURE_REQUEST_WAIT_TIME_SECONDS_KEY: "600",
    CONFIG_TIMEOUT_KEY: "21600",
    CONFIG_MAX_WAIT_INTERVAL_KEY: "60",
    # Authentication Provider - Google OpenID/OAuth
    CONFIG_AUTH_PROVIDER: "google",
}
