import os
import signal
import platform
import socket
import subprocess
import atexit
import time
from typing import Union, List, Dict, Any
import grpc
from feast import errors
from feast.type_map import python_values_to_proto_values
from feast.feature_service import FeatureService
from feast.repo_config import RepoConfig
from feast.online_response import OnlineResponse
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesRequest, GetFeastServingInfoRequest
from feast.protos.feast.serving.ServingService_pb2_grpc import ServingServiceStub
import feast


class GoServer:
    FeatureNameCollisionError_STRING = "FeatureNameCollisionError"
    ValueError_STRING = "ValueError"

    def __init__(self, repo_path: str, config: RepoConfig):
        self._repo_path = repo_path
        self._config = config
        self.grpcServerStarted = False
        self.httpServerStarted = False
        self.pipeClosed = False
        self._connect()

    def get_online_features(
            self,
            features: Union[List[str], FeatureService],
            entities: Dict[str, List[Any]],
            full_feature_names: bool = False,
    ) -> OnlineResponse:

        if not self.grpcServerStarted:
            self._connect()

        request = GetOnlineFeaturesRequest(full_feature_names=full_feature_names)
        if isinstance(features, FeatureService):
            request.feature_service = features.name
        elif isinstance(features, list):
            request.features.val.extend(features)
        else:
            errors.InvalidFeaturesParameterType(features)

        for key, values in entities.items():
            request.entities[key].val.extend(python_values_to_proto_values(values))
        
        try:
            response = self.client.GetOnlineFeatures(request=request)
        except grpc.RpcError as rpc_error:
            # If the server became unavailable, it could mean that the subprocess died or fell into a bad state
            # So the resolution is to establish a new Go subprocess and set up a new connection with it

            # Socket might not have closed
            # if this is a grpc problem
            self.stop()
            if rpc_error.code() == grpc.StatusCode.UNAVAILABLE:
                self._connect()
                # Retry request with the new Go subprocess
                response = self.client.GetOnlineFeatures(request=request)
            else:
                error_message = rpc_error.details()
                if error_message.startswith(self.FeatureNameCollisionError_STRING):
                    parsed_error_message = error_message.split(": ")[1].split("; ")
                    collided_feature_refs = parsed_error_message[0].split(", ")
                    full_feature_names = parsed_error_message[1] == "true"
                    raise errors.FeatureNameCollisionError(collided_feature_refs, full_feature_names)
                elif error_message.startswith(self.ValueError_STRING):
                    parsed_error_message = error_message.split(": ")[1]
                    raise ValueError(parsed_error_message)
                else:
                    raise

        return OnlineResponse(response)

    def _connect(self):
        """Start Go subprocess on a random unused port and connect to it"""
        unused_port = _get_unused_port()
        self.unused_port = unused_port
        env = {
            "FEAST_REPO_CONFIG": self._config.json(),
            "FEAST_REPO_PATH": self._repo_path,
            # pass a random unused port to go subprocess, so that there's no conflicts
            # if multiple Python processes start Go subprocess on the same host
            "FEAST_GRPC_PORT": unused_port,
            **os.environ
        }
        cwd = feast.__path__[0]

        if "dev" in feast.__version__:
            self.process = subprocess.Popen(["go", "run", "github.com/feast-dev/feast/go/server"],
                                            cwd=cwd, env=env,
                                            stdin=subprocess.PIPE )
        else:
            goos = platform.system().lower()
            goarch = "amd64" if platform.machine() == "x86_64" else "arm64"
            executable = feast.__path__[0] + f"/binaries/go_server_{goos}_{goarch}"
            self.process = subprocess.Popen([executable], cwd=cwd, env=env, stdin=subprocess.PIPE)

        # Make sure the subprocess is terminated when the parent process dies
        # Note: this doesn't handle cases where the parent process is abruptly killed (e.g. with SIGKILL)
        atexit.register(lambda: self.stop() )
        self.start_grpc_server()
        self.pipeClosed = False

    def start_grpc_server(self):
        if self.grpcServerStarted:
            return
        # Try connecting to the go server using a gPRC client
        
        for i in range(5):
            try:
                self.process.stdin.write(b'startGrpc\n')
                self.process.stdin.flush()
                self.grpcServerStarted = True
                break
            except subprocess.CalledProcessError as error:
                # If there is an exception
                # go subprocess probably closed so pipe is closed
                self.pipeClosed = True
                self.stop()
                raise errors.GoSubprocessConnectionFailed() from error
        channel = grpc.insecure_channel(f"127.0.0.1:{self.unused_port}")
        self.client = ServingServiceStub(channel)

        # Make sure the connection can be used for feature retrieval before returning from constructor
        # We try connecting to the Go subprocess for 5 seconds (50 times with 0.1 second intervals)
        for i in range(50):
            try:
                self.client.GetFeastServingInfo(request=GetFeastServingInfoRequest())
                break
            except Exception as e:
                if i == 49:
                    self.stop()
                    raise errors.GoSubprocessConnectionFailed() from e
                # Sleep for 0.1 second before retrying
            time.sleep(0.1)

    def start_http_server(self, host: str, port: int):
        if self.httpServerStarted:
            return
        for i in range(10):
            try:
                self.process.stdin.write(bytes(f"startHttp {host}:{port}\n", encoding='utf8'))
                self.process.stdin.flush()
                self.httpServerStarted = True
                break
            except subprocess.CalledProcessError as error:
                # If there is an exception
                # go subprocess probably closed so pipe is closed
                self.pipeClosed = True
                self.stop()
                raise errors.GoSubprocessConnectionFailed() from error

    def stop(self):

        # Only send sigkill if there's a problem telling go subprocess to stop
        # Otherwise, let go subprocess clean up and shut down itself
        if not self.pipeClosed:
            try:
                self.process.stdin.write(bytes(f"stop\n", encoding='utf8'))
                self.process.stdin.flush()
                # TODO (Ly): Review: We don't close stdin here
                # since if the call succeeds go process closes
                # itself and stdin?
                # self.process.stdin.close()
            except subprocess.CalledProcessError as error:
                self.process.terminate()
                raise errors.GoSubprocessConnectionFailed() from error
       
        self.grpcServerStarted = False
        self.httpServerStarted = False
        self.pipeClosed = True
                
def _get_unused_port() -> str:
    sock = socket.socket()
    # binding port 0 means os will choose an unused port for us
    sock.bind(("", 0))
    # get the chosen port by OS
    _, port = sock.getsockname()
    # close the port so it can be used by Go subprocess
    sock.close()
    return str(port)
