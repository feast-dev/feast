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
import ctypes
import os
import pathlib
import platform
import shutil
import signal
import socket
import subprocess
import threading
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


class GoServerConnection:
    def __init__(self, config: RepoConfig, repo_path: str, go_server_port: int = -1):
        self._process: Optional[Popen[bytes]] = None
        self._config = config
        self._repo_path = repo_path
        self._go_server_port = go_server_port

    def _get_unused_port(self) -> str:
        if is_test():
            return str(self._go_server_port)

        port = 54321

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            while True:
                # Break once found an unused port starting with 54321
                if s.connect_ex(("localhost", port)) != 0:
                    break
                port += 1

        return str(port)

    def connect(self) -> bool:
        self.unused_port = self._get_unused_port()
        env = {
            "FEAST_REPO_CONFIG": self._config.json(),
            "FEAST_REPO_PATH": self._repo_path,
            "FEAST_GRPC_PORT": self.unused_port,
            **os.environ,
        }
        cwd = feast.__path__[0]
        goos = platform.system().lower()
        goarch = "amd64" if platform.machine() == "x86_64" else "arm64"
        executable = feast.__path__[0] + f"/binaries/goserver_{goos}_{goarch}"
        # Automatically reconnect with go subprocess exits
        self._process = Popen([executable], cwd=cwd, env=env,)

        channel = grpc.insecure_channel(f"127.0.0.1:{self.unused_port}")
        self.client: ServingServiceStub = ServingServiceStub(channel)

        try:
            self._check_grpc_connection()
            return True
        except grpc.RpcError:
            return False

    def kill_process(self):
        if self._process:
            # self._process.terminate()
            self._process.send_signal(signal.SIGINT)

    def is_process_alive(self):
        return self._process and self._process.poll()

    def wait_for_process(self, timeout):
        self._process.wait(timeout)

    def set_port(self, port):
        self._port = port

    # Make sure the connection can be used for feature retrieval before returning from
    # constructor. We try connecting to the Go subprocess for 5 seconds or at most 50 times
    @retry(
        stop=(stop_after_delay(10) | stop_after_attempt(50)),
        wait=wait_exponential(multiplier=0.1, min=0.1, max=5),
    )
    def _check_grpc_connection(self):
        self.client.GetFeastServingInfo(request=GetFeastServingInfoRequest())


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

    def __init__(self, repo_path: str, config: RepoConfig, go_server_port: int = -1):
        """Creates a GoServer object."""
        self._repo_path = repo_path
        self._config = config
        self._go_server_started = threading.Event()
        if is_test():
            use_go_server_thread = os.getenv("USE_GO_SERVER_THREAD") == "True"
            go_server_port += use_go_server_thread
        self._shared_connection = GoServerConnection(config, repo_path, go_server_port)
        self._dev_mode = "dev" in feast.__version__
        if not is_test() and self._dev_mode:
            self._build_binaries()

        if self._check_use_thread():
            self._start_go_server_use_thread()
        else:
            self._start_go_server()

    def _check_use_thread(self):
        use_go_server_thread = os.getenv("USE_GO_SERVER_THREAD") == "True"
        return use_go_server_thread

    def set_port(self, port):
        self._shared_connection.set_port(port)

    def _build_binaries(self):

        goos = platform.system().lower()
        goarch = "amd64" if platform.machine() == "x86_64" else "arm64"
        binaries_path = (pathlib.Path(__file__).parent / "../feast/binaries").resolve()
        binaries_path_abs = str(binaries_path.absolute())
        if binaries_path.exists():
            shutil.rmtree(binaries_path_abs)
        os.mkdir(binaries_path_abs)

        subprocess.check_output(
            [
                "go",
                "build",
                "-o",
                f"{binaries_path_abs}/goserver_{goos}_{goarch}",
                "github.com/feast-dev/feast/go/cmd/goserver",
            ],
            env={"GOOS": goos, "GOARCH": goarch, **os.environ},
        )

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
        if self._check_use_thread() and not self._go_server_started.is_set():
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
                if not self._check_use_thread():
                    self._start_go_server()
                elif not self._go_server_started.is_set():
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

        self._go_server_background_thread = GoServerBackgroundThread(
            "GoServerBackgroundThread",
            self._shared_connection,
            self._go_server_started,
        )
        self._go_server_background_thread.start()
        atexit.register(lambda: self._go_server_background_thread.stop_go_server())
        signal.signal(
            signal.SIGTERM,
            lambda sig, frame: self._go_server_background_thread.stop_go_server(),
        )
        signal.signal(
            signal.SIGINT,
            lambda sig, frame: self._go_server_background_thread.stop_go_server(),
        )

        # Wait for go server subprocess to start for the first time before returning
        self._go_server_started.wait()

    def _start_go_server(self):
        if self._shared_connection.is_process_alive():
            self._shared_connection.kill_process()

        self._shared_connection.connect()
        atexit.register(lambda: self._shared_connection.kill_process())
        signal.signal(
            signal.SIGTERM, lambda sig, frame: self._shared_connection.kill_process()
        )
        signal.signal(
            signal.SIGINT, lambda sig, frame: self._shared_connection.kill_process()
        )

    def kill_go_server_explicitly(self):
        if self._check_use_thread():
            self._go_server_background_thread.stop_go_server()
        else:
            self._shared_connection.kill_process()


# https://www.geeksforgeeks.org/python-different-ways-to-kill-a-thread/
class GoServerBackgroundThread(threading.Thread):
    def __init__(
        self,
        name: str,
        shared_connection: GoServerConnection,
        go_server_started: threading.Event,
    ):
        threading.Thread.__init__(self)
        self.name = name
        self._shared_connection = shared_connection
        self._go_server_started = go_server_started

    def run(self):
        # Target function of the thread class
        try:
            while True:
                self._go_server_started.clear()

                # If we fail to connect to grpc stub, terminate subprocess and repeat
                if not self._shared_connection.connect():
                    self._shared_connection.kill_process()
                    continue
                self._go_server_started.set()
                while True:
                    try:
                        # Making a blocking wait by setting timeout to a very long time so we don't waste cpu cycle
                        self._shared_connection.wait_for_process(3600)
                    except subprocess.TimeoutExpired:
                        pass
                    if not self._shared_connection.is_process_alive():
                        break
        finally:
            # Main thread exits
            self._shared_connection.kill_process()

    def stop_go_server(self):
        thread_id = self._get_id()
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            thread_id, ctypes.py_object(SystemExit)
        )
        # TODO: Review that kill process here but run also has to stop
        if res > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
        else:
            self._shared_connection.kill_process()

    def _get_id(self):
        # returns id of the respective thread
        if hasattr(self, "_thread_id"):
            return self._thread_id
        for id, thread in threading._active.items():
            if thread is self:
                return id
