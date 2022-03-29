# Copyright 2022 The Feast Authors
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
import atexit
import logging
import os
import platform
import random
import string
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from subprocess import Popen
from typing import Any, Dict, List, Optional, Union

import grpc
from tenacity import retry, stop_after_attempt, stop_after_delay, wait_exponential

import feast
from feast.errors import FeatureNameCollisionError, InvalidFeaturesParameterType
from feast.feature_service import FeatureService
from feast.flags_helper import is_test
from feast.online_response import OnlineResponse
from feast.protos.feast.serving.ServingService_pb2 import (
    GetFeastServingInfoRequest,
    GetOnlineFeaturesRequest,
)
from feast.protos.feast.serving.ServingService_pb2_grpc import ServingServiceStub
from feast.repo_config import RepoConfig
from feast.type_map import python_values_to_proto_values

_logger = logging.getLogger(__name__)


class GoServerConnection:
    def __init__(self, config: RepoConfig, repo_path: str):
        self._process: Optional[Popen[bytes]] = None
        self._config = config
        self._repo_path = repo_path
        self.temp_dir = tempfile.TemporaryDirectory()
        self._client: Optional[ServingServiceStub] = None

    @property
    def client(self):
        if self._client:
            return self._client
        raise RuntimeError("Client not established with go subprocess")

    def _get_unix_domain_file_path(self) -> Path:
        # This method should return a file that go server should listen on and that the python channel
        # should communicate to.
        now = time.time_ns()
        letters = string.ascii_lowercase
        random_suffix = "".join(random.choice(letters) for _ in range(10))

        return Path(self.temp_dir.name, f"{now}_{random_suffix}.sock")

    def connect(self) -> bool:
        self.sock_file = self._get_unix_domain_file_path()
        env = {
            "FEAST_REPO_CONFIG": self._config.json(),
            "FEAST_REPO_PATH": self._repo_path,
            "FEAST_GRPC_SOCK_FILE": str(self.sock_file),
            **os.environ,
        }
        cwd = feast.__path__[0]
        goos = platform.system().lower()
        goarch = "amd64" if platform.machine() == "x86_64" else "arm64"
        executable = (
            feast.__path__[0] + f"/binaries/server_{goos}_{goarch}"
            if not is_test()
            else feast.__path__[0] + "/binaries/server"
        )
        # Automatically reconnect with go subprocess exits
        self._process = Popen([executable], cwd=cwd, env=env,)

        channel = grpc.insecure_channel(f"unix:{self.sock_file}")
        self._client = ServingServiceStub(channel)

        try:
            self._check_grpc_connection()
            return True
        except grpc.RpcError:
            return False

    def kill_process(self):
        if self._process:
            self._process.terminate()

    def is_process_alive(self):
        return self._process and self._process.poll() is None

    def wait_for_process(self, timeout):
        self._process.wait(timeout)

    # Make sure the connection can be used for feature retrieval before returning from
    # constructor. We try connecting to the Go subprocess for 5 seconds or at most 50 times
    @retry(
        stop=(stop_after_delay(10) | stop_after_attempt(50)),
        wait=wait_exponential(multiplier=0.1, min=0.1, max=5),
    )
    def _check_grpc_connection(self):
        return self.client.GetFeastServingInfo(request=GetFeastServingInfoRequest())


class GoServer:
    """
    A GoServer defines a thin Python wrapper around a Go gRPC server that retrieves and
    serves online features.

    Attributes:
        _repo_path: The path to the Feast repo for which this go server is defined.
        _config: The RepoConfig for the Feast repo for which this go server is defined.
    """

    _repo_path: str
    _config: RepoConfig

    def __init__(self, repo_path: str, config: RepoConfig):
        """Creates a GoServer object."""
        self._repo_path = repo_path
        self._config = config
        self._go_server_started = threading.Event()
        self._shared_connection = GoServerConnection(config, repo_path)
        self._dev_mode = "dev" in feast.__version__

        self._start_go_server_use_thread()

    def get_online_features(
        self,
        features: Union[List[str], FeatureService],
        entities: Dict[str, List[Any]],
        full_feature_names: bool = False,
    ) -> OnlineResponse:
        """
        Retrieves the latest online feature data.

        Args:
            features: Either a list of feature references or a feature service that
                determines which features will be retrieved. Feature references should
                be of the form "feature_view:feature".
            entity_rows: A list of dictionaries where each key-value pair is an
                entity-name or entity-value pair.
            full_feature_names: Whether feature names should be returned with feature
                view names as prefixes, changing them from the format "feature" to
                "feature_view__feature".

        Returns:
            An OnlineResponse containing the feature data.

        Raises:
            InvalidFeaturesParameterType: If features is not a list or a feature service.
            FeatureNameCollisionError: If a feature reference is specified multiple times.
            ValueError: If some other error occurs.
        """
        # Wait for go server subprocess to restart before asking for features
        if not self._go_server_started.is_set():
            self._go_server_started.wait()

        request = GetOnlineFeaturesRequest(full_feature_names=full_feature_names)
        if isinstance(features, FeatureService):
            request.feature_service = features.name
        elif isinstance(features, list):
            request.features.val.extend(features)
        else:
            raise InvalidFeaturesParameterType(features)

        for key, values in entities.items():
            request.entities[key].val.extend(python_values_to_proto_values(values))

        try:
            response = self._shared_connection.client.GetOnlineFeatures(request=request)
        except grpc.RpcError as rpc_error:
            # Socket might not have closed if this is a grpc problem.
            if rpc_error.code() == grpc.StatusCode.UNAVAILABLE:
                # If the server became unavailable, it could mean that the subprocess died or fell
                # into a bad state, so the resolution is to wait for go server to restart in the background
                if not self._go_server_started.is_set():
                    self._go_server_started.wait()
                # Retry request with the new Go subprocess
                response = self._shared_connection.client.GetOnlineFeatures(
                    request=request
                )
            else:
                error_message = rpc_error.details()
                if error_message.lower().startswith(
                    FeatureNameCollisionError.__name__.lower()
                ):
                    parsed_error_message = error_message.split(": ")[1].split("; ")
                    collided_feature_refs = parsed_error_message[0].split(", ")
                    full_feature_names = parsed_error_message[1] == "true"
                    raise FeatureNameCollisionError(
                        collided_feature_refs, full_feature_names
                    )
                elif error_message.lower().startswith(ValueError.__name__.lower()):
                    parsed_error_message = error_message.split(": ")[1]
                    raise ValueError(parsed_error_message)
                else:
                    raise

        return OnlineResponse(response)

    def _start_go_server_use_thread(self):

        self._go_server_background_thread = GoServerMonitorThread(
            "GoServerMonitorThread", self._shared_connection, self._go_server_started
        )
        self._go_server_background_thread.start()
        atexit.register(lambda: self._go_server_background_thread.stop())

        # Wait for go server subprocess to start for the first time before returning
        self._go_server_started.wait()

    def kill_go_server_explicitly(self):
        self._go_server_background_thread._is_cancelled.set()
        self._go_server_background_thread.stop()
        self._go_server_background_thread.join()


class GoServerMonitorThread(threading.Thread):
    def __init__(
        self,
        name: str,
        shared_connection: GoServerConnection,
        go_server_first_started: threading.Event,
    ):
        threading.Thread.__init__(self)
        self.name = name
        self._shared_connection = shared_connection
        self._is_cancelled = threading.Event()
        self.daemon = True
        self._go_server_started = go_server_first_started

    def run(self):
        # Target function of the thread class
        _logger.debug(
            "%s Started monitoring thread to keep go feature server alive", self.ident
        )
        try:
            while not self._is_cancelled.is_set():

                # If we fail to connect to grpc stub, terminate subprocess and repeat
                _logger.debug("%s Connecting to subprocess", self.ident)
                if not self._shared_connection.connect():
                    _logger.debug(
                        "%s Failed to connect, killing and retrying", self.ident
                    )
                    self._shared_connection.kill_process()
                    continue
                else:
                    _logger.debug(
                        "%s Go feature server started, process: %s",
                        self.ident,
                        self._shared_connection._process.pid,
                    )
                    self._go_server_started.set()
                _logger.debug(
                    "%s is_cancelled status: %s", self.ident, self._is_cancelled
                )
                while not self._is_cancelled.is_set():
                    try:
                        # Making a blocking wait by setting timeout to a very long time so we don't waste cpu cycle
                        self._shared_connection.wait_for_process(3600)
                    except subprocess.TimeoutExpired:
                        pass
                    _logger.debug(
                        "%s No longer waiting for process: %s, %s, %s",
                        self.ident,
                        self._shared_connection._process.pid,
                        self._shared_connection._process.returncode,
                        self._shared_connection.is_process_alive(),
                    )
                    if not self._shared_connection.is_process_alive():
                        break
        finally:
            # Main thread exits
            self._shared_connection.kill_process()

    def stop(self):
        _logger.debug(
            "%s Stopping monitoring thread and terminating go feature server",
            self.ident,
        )
        self._is_cancelled.set()
        self._shared_connection.kill_process()
