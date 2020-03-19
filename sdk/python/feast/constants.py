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

# General constants
DATETIME_COLUMN = "datetime"
FEAST_CONFIG_FILE_ENV_KEY = "FEAST_CONFIG"
CONFIG_FEAST_ENV_VAR_PREFIX = "FEAST_"
CONFIG_FILE_DEFAULT_DIRECTORY = ".feast"
CONFIG_FILE_NAME = "config"
CONFIG_FILE_SECTION = "general"


# Feast configuration options
CONFIG_CORE_URL_KEY = "core_url"
CONFIG_SERVING_URL_KEY = "serving_url"
CONFIG_PROJECT_KEY = "project"
CONFIG_CORE_SECURE_KEY = "core_secure"
CONFIG_SERVING_SECURE_KEY = "serving_secure"
CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY = "grpc_connection_timeout_default"
CONFIG_GRPC_CONNECTION_TIMEOUT_APPLY_KEY = "grpc_connection_timeout_apply_key"
CONFIG_BATCH_FEATURE_REQUEST_WAIT_TIME_SECONDS_KEY = (
    "batch_feature_request_wait_time_seconds"
)

# Configuration option default values
FEAST_DEFAULT_OPTIONS = {
    CONFIG_PROJECT_KEY: "default",
    CONFIG_CORE_URL_KEY: "localhost:6565",
    CONFIG_CORE_SECURE_KEY: "False",
    CONFIG_SERVING_URL_KEY: "localhost:6565",
    CONFIG_SERVING_SECURE_KEY: "False",
    CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY: "3",
    CONFIG_GRPC_CONNECTION_TIMEOUT_APPLY_KEY: "600",
    CONFIG_BATCH_FEATURE_REQUEST_WAIT_TIME_SECONDS_KEY: "600",
}
