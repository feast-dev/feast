import select
import socket
import threading
from collections import defaultdict, namedtuple
from typing import Dict

import requests

MetricLine = namedtuple("MetricLine", ["name", "value", "type"])


class StatsDServer:
    host: str
    port: int
    metrics: Dict[str, int]


class PrometheusStatsDServer(StatsDServer):
    def __init__(
        self,
        statsd_host: str,
        statsd_port: int,
        prometheus_host: str,
        prometheus_port: int,
    ):
        self.host = statsd_host
        self.port = statsd_port

        self.prometheus_host = prometheus_host
        self.prometheus_port = prometheus_port

    @property
    def metrics(self):
        """Parse Prometheus response into metrics dict"""

        data = requests.get(
            f"http://{self.prometheus_host}:{self.prometheus_port}/metrics"
        ).content.decode()
        lines = [line for line in data.split("\n") if not line.startswith("#")]
        metrics = {}
        for line in lines:
            if not line:
                continue

            name, value = line.split(" ")

            try:
                value = int(value)  # type: ignore
            except ValueError:
                value = float(value)  # type: ignore

            if "{" in name and "}" in name:
                base = name[: name.index("{")]
                tags = name[name.index("{") + 1 : -1]
                tags = [tag.split("=") for tag in tags.split(",")]
                tags = [(key, val.replace('"', "")) for key, val in tags]

                name = base + "#" + ",".join(f"{k}:{v}" for k, v in sorted(tags))

            metrics[name] = value

        return metrics


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
        tags = sorted(parts[2][1:].split(","))
        name = name + "#" + ",".join(tags)

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
        sock.setblocking(False)

        while True:
            ready = select.select([sock], [], [], 1)
            if ready[0]:
                data = sock.recv(65535)

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
