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
import os
import platform
import socket
import subprocess
import time
from typing import Any, Dict, List, Union

import grpc

import feast
from feast.errors import (
    FeatureNameCollisionError,
    GoSubprocessConnectionFailed,
    InvalidFeaturesParameterType,
)
from feast.feature_service import FeatureService
from feast.online_response import OnlineResponse
from feast.protos.feast.serving.ServingService_pb2 import (
    GetFeastServingInfoRequest,
    GetOnlineFeaturesRequest,
)
from feast.protos.feast.serving.ServingService_pb2_grpc import ServingServiceStub
from feast.repo_config import RepoConfig
from feast.type_map import python_values_to_proto_values


class GoServer:
    """
    A GoServer defines a thin Python wrapper around a Go gRPC server that retrieves and
    serves online features.

    Attributes:
        _repo_path: The path to the Feast repo for which this go server is defined.
        _config: The RepoConfig for the Feast repo for which this go server is defined.
        grpc_server_started: Whether the gRPC server has been started.
        pipe_closed: Whether the pipe to the Go subprocess has been closed.
    """

    _repo_path: str
    _config: RepoConfig
    grpc_server_started: bool
    pipe_closed: bool

    def __init__(self, repo_path: str, config: RepoConfig):
        """Creates a GoServer object."""
        self._repo_path = repo_path
        self._config = config
        self.grpc_server_started = False
        self.pipe_closed = False
        self._connect()

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
        if not self.grpc_server_started:
            self._connect()

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
            response = self.client.GetOnlineFeatures(request=request)
        except grpc.RpcError as rpc_error:
            # If the server became unavailable, it could mean that the subprocess died
            # or fell into a bad state, so the resolution is to establish a new Go
            # subprocess and set up a new connection with it.

            # Socket might not have closed if this is a grpc problem.
            self.stop()
            if rpc_error.code() == grpc.StatusCode.UNAVAILABLE:
                self._connect()
                # Retry request with the new Go subprocess
                response = self.client.GetOnlineFeatures(request=request)
            else:
                error_message = rpc_error.details()
                if error_message.startswith(FeatureNameCollisionError.__name__):
                    parsed_error_message = error_message.split(": ")[1].split("; ")
                    collided_feature_refs = parsed_error_message[0].split(", ")
                    full_feature_names = parsed_error_message[1] == "true"
                    raise FeatureNameCollisionError(
                        collided_feature_refs, full_feature_names
                    )
                elif error_message.startswith(ValueError.__name__):
                    parsed_error_message = error_message.split(": ")[1]
                    raise ValueError(parsed_error_message)
                else:
                    raise

        return OnlineResponse(response)

    def _connect(self):
        """Start the Go subprocess on a random unused port and connect to it."""
        # We pass a random unused port to the Go subprocess, so there are no conflicts
        # if multiple Python processes start a Go subprocess on the same host
        self.unused_port = _get_unused_port()
        env = {
            "FEAST_REPO_CONFIG": self._config.json(),
            "FEAST_REPO_PATH": self._repo_path,
            "FEAST_GRPC_PORT": self.unused_port,
            **os.environ,
        }
        cwd = feast.__path__[0]

        if "dev" in feast.__version__:
            self.process = subprocess.Popen(
                ["go", "run", "github.com/feast-dev/feast/go/server"],
                cwd=cwd,
                env=env,
                stdin=subprocess.PIPE,
            )
        else:
            goos = platform.system().lower()
            goarch = "amd64" if platform.machine() == "x86_64" else "arm64"
            executable = feast.__path__[0] + f"/binaries/go_server_{goos}_{goarch}"
            self.process = subprocess.Popen(
                [executable], cwd=cwd, env=env, stdin=subprocess.PIPE
            )

        # Make sure the subprocess is terminated when the parent process dies
        # Note: this doesn't handle cases where the parent process is abruptly
        # killed (e.g. with SIGKILL)
        atexit.register(lambda: self.stop())
        self.start_grpc_server()
        self.pipe_closed = False

    def start_grpc_server(self):
        """Start the gRPC server on the Go subprocess."""
        if self.grpc_server_started:
            return

        # Try 5 times to wait for pipe to open
        for i in range(5):
            try:
                self.process.stdin.write(b"startGrpc\n")
                self.process.stdin.flush()
                self.grpc_server_started = True
                break
            except subprocess.CalledProcessError as error:
                # If there is an exception the Go subprocess is probably closed
                # so the pipe is also probably closed.
                self.pipe_closed = True
                self.stop()
                raise GoSubprocessConnectionFailed() from error

        channel = grpc.insecure_channel(f"127.0.0.1:{self.unused_port}")
        self.client = ServingServiceStub(channel)

        # Make sure the connection can be used for feature retrieval before returning
        # from constructor. We try connecting to the Go subprocess for 5 seconds (50
        # times with 0.1 second intervals).
        # TODO (Ly): Review: increase time.sleep(0.2) to 0.2 so wait 10 seconds
        # since python connector plugin can take up to 8 seconds to spin up.
        # 5 seconds is not enough sometimes to spin up grpc server itself
        for i in range(50):
            try:
                self.client.GetFeastServingInfo(request=GetFeastServingInfoRequest())
                break
            except Exception as e:
                if i == 49:
                    self.stop()
                    raise GoSubprocessConnectionFailed() from e

            # Sleep for 0.1 second before retrying.
            time.sleep(0.1)

    def stop(self):
        """Stop the gRPC server on the Go subprocess and ensure the process is dead."""
        if not self.pipe_closed:
            try:
                # Let the Go subprocess clean up and shut down itself.
                self.process.stdin.write(bytes("stop\n", encoding="utf8"))
                self.process.stdin.flush()
                # TODO (Ly): Review: We don't close stdin here
                # since if the call succeeds go process closes
                # itself and stdin?
                # self.process.stdin.close()
            except subprocess.CalledProcessError as error:
                # If there is a problem telling the Go subprocess to stop,
                # directly terminate it.
                if not isinstance(error, BrokenPipeError):
                    self.process.terminate()
                    raise
                
        self.grpc_server_started = False
        self.pipe_closed = True


def _get_unused_port() -> str:
    sock = socket.socket()

    # Binding to port 0 means the OS will choose an unused port
    sock.bind(("", 0))

    # Get the port chosen by the OS
    _, port = sock.getsockname()

    # Close the port so it can be used by the Go subprocess
    sock.close()
    return str(port)
