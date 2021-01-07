import socket
import threading
from collections import defaultdict, namedtuple
from typing import Dict

MetricLine = namedtuple("MetricLine", ["name", "value", "type"])


class StatsDServer:
    host: str
    port: int
    metrics: Dict[str, int]


class RemoteStatsDServer(StatsDServer):
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.metrics = {}


def parse_metric_line(line: str) -> MetricLine:
    parts = line.split("|")
    name, value = parts[0].split(":")
    type_ = parts[1]

    try:
        value = int(value)  # type: ignore
    except ValueError:
        value = float(value)  # type: ignore

    if len(parts) == 3 and parts[2].startswith("#"):
        # Add tags to name
        name = name + parts[2]

    return MetricLine(name, value, type_)


class StatsDStub(StatsDServer):
    def __init__(self, port: int):
        self.host = "localhost"
        self.port = port

        self._stop_event = threading.Event()
        self.metrics = defaultdict(lambda: 0)

    def serve(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((self.host, self.port))

        while True:
            data, _ = sock.recvfrom(65535)

            lines = data.decode("utf-8").split("\n")
            for line in lines:
                print("Metric received:", line)
                m = parse_metric_line(line)
                self.metrics[m.name] += m.value

            if self._stop_event.wait(0.01):
                break

    def start(self):
        t = threading.Thread(target=self.serve)
        t.start()

    def stop(self):
        self._stop_event.set()
