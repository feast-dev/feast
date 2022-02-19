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
    def __init__(self, repo_path: str, config: RepoConfig):
        self._repo_path = repo_path
        self._config = config
        self.grpcServerStarted = False
        self.httpServerStarted = False
        self.pipeClosed = False
        self._connect()
        print("start grpc conenction")

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

        # print("GoServer get_online_features")
        for key, values in entities.items():
            # print(key)
            request.entities[key].val.extend(python_values_to_proto_values(values))
        # print("==============")
        
        try:
            response = self.client.GetOnlineFeatures(request=request)
        except grpc.RpcError as rpc_error:
            # If the server became unavailable, it could mean that the subprocess died or fell into a bad state
            # So the resolution is to establish a new Go subprocess and set up a new connection with it
            self.stop()
            if rpc_error.code() == grpc.StatusCode.UNAVAILABLE:
                self._connect()
                # Retry request with the new Go subprocess
                response = self.client.GetOnlineFeatures(request=request)
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
                self.stop()
                raise errors.GoSubprocessConnectionFailed() from error

    def stop(self):

        if self.pipeClosed:
            return

        for i in range(10):
            try:
                self.process.stdin.write(bytes(f"stop\n", encoding='utf8'))
                self.process.stdin.flush()
                break
            except subprocess.CalledProcessError as error:
                self.process.terminate()
                raise errors.GoSubprocessConnectionFailed() from error 
        # self.process.terminate()
        self.process.stdin.close()
        # os.killpg(os.getpgid(self.process.pid), signal.SIGINT)
        # self.process.terminate()
        self.grpcServerStarted = False
        self.httpServerStarted = False
        self.pipeClosed = True
        print("stop grpc connection")
                
def _get_unused_port() -> str:
    sock = socket.socket()
    # binding port 0 means os will choose an unused port for us
    sock.bind(("", 0))
    # get the chosen port by OS
    _, port = sock.getsockname()
    # close the port so it can be used by Go subprocess
    sock.close()
    return str(port)
