"""Run this repository's selected tests against an isolated LocalCloud container."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import ProxyHandler, Request, build_opener

ROOT = Path(__file__).resolve().parents[1]


def docker(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["docker", *args], text=True, capture_output=True, check=False
    )
    if check and result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--full-suite",
        action="store_true",
        help="run the normal regression suite instead of the selected service tests",
    )
    parser.add_argument(
        "--existing",
        metavar="GATEWAY",
        help="borrow an already running LocalCloud instead of starting a container",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="dependencies/build were already prepared",
    )
    args = parser.parse_args()
    manifest = json.loads((ROOT / ".localcloud/tests.json").read_text())
    ident = "lc-tests-" + uuid.uuid4().hex[:12]
    container_created = False
    volume_created = False
    project_owned = False
    failed = True
    gateway = args.existing
    ports: dict[int, int] = {}
    supplied = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
    project = supplied or "lc-tests-" + uuid.uuid4().hex[:12]
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,29}", project):
        raise RuntimeError("Invalid LocalCloud project ID")
    opener = build_opener(ProxyHandler({}))

    def request(method: str, path: str, body: dict | None = None) -> object:
        payload = None if body is None else json.dumps(body).encode()
        req = Request(
            str(gateway).rstrip("/") + path,
            data=payload,
            method=method,
            headers={
                "Content-Type": "application/json",
                "X-LocalCloud-User": "repository-tests",
            },
        )
        with opener.open(req, timeout=30) as response:
            return json.load(response)

    def mapped_endpoint(value: str) -> str:
        is_url = "://" in value
        parsed = urlsplit(value if is_url else "//" + value)
        port = parsed.port
        if args.existing and parsed.hostname in {"localhost", "127.0.0.1"}:
            return value
        if (
            parsed.hostname not in {"localhost", "127.0.0.1", "0.0.0.0"}
            or port not in ports
        ):
            raise RuntimeError(
                "LocalCloud advertised an endpoint outside its published container ports"
            )
        authority = "127.0.0.1:" + str(ports[port])
        return (
            urlunsplit(
                (parsed.scheme, authority, parsed.path, parsed.query, parsed.fragment)
            )
            if is_url
            else authority
        )

    try:
        if gateway is None:
            docker("info", "--format", "{{.ServerVersion}}")
            docker(
                "volume", "create", "--label", "localcloud.test-owner=" + ident, ident
            )
            volume_created = True
            command = [
                "create",
                "--name",
                ident,
                "--label",
                "localcloud.test-owner=" + ident,
                "--memory",
                "4g",
                "-v",
                ident + ":/var/lib/localcloud",
            ]
            for port in manifest["ports"]:
                command += ["-p", "127.0.0.1::" + str(port)]
            command.append(manifest["image"])
            docker(*command)
            container_created = True
            docker(
                "cp",
                str(ROOT / ".localcloud/config.json"),
                ident + ":/etc/localcloud/localcloud.yaml",
            )
            docker("start", ident)
            info = json.loads(docker("inspect", ident))[0]
            ports = {
                int(port.split("/")[0]): int(bindings[0]["HostPort"])
                for port, bindings in info["NetworkSettings"]["Ports"].items()
                if bindings
            }
            gateway = "http://127.0.0.1:" + str(ports[manifest["gateway_port"]])
        deadline = time.monotonic() + 240
        while True:
            try:
                readiness = request("GET", "/readiness")
                if isinstance(readiness, dict) and readiness.get("ready") is True:
                    request("GET", "/projects")
                    break
            except (OSError, ValueError):
                pass
            if (
                container_created
                and docker("inspect", "--format", "{{.State.Running}}", ident) != "true"
            ):
                raise RuntimeError("LocalCloud container exited during startup")
            if time.monotonic() >= deadline:
                raise RuntimeError("LocalCloud did not become ready within 240 seconds")
            time.sleep(2)
        services = request("GET", "/services")
        rows = services.get("services", []) if isinstance(services, dict) else services
        enabled = {
            s.get("service_id", s.get("id")) for s in rows if s.get("enabled") is True
        }
        if enabled != set(manifest["services"]):
            raise RuntimeError(
                "LocalCloud enabled services differ from this repository's test contract: "
                + str(sorted(enabled))
            )
        try:
            request("POST", "/projects", {"project_id": project})
            project_owned = not supplied
        except HTTPError as exc:
            if exc.code != 409 or not supplied:
                raise
        advertised = request("GET", "/env?format=json&project=" + project)
        if (
            not isinstance(advertised, dict)
            or advertised.get("GOOGLE_CLOUD_PROJECT") != project
        ):
            raise RuntimeError("LocalCloud did not advertise the selected project")
        env = {
            k: v
            for k, v in os.environ.items()
            if not k.endswith("_EMULATOR_HOST")
            and k
            not in {
                "GOOGLE_APPLICATION_CREDENTIALS",
                "GOOGLE_CLOUD_PROJECT",
                "GCLOUD_PROJECT",
                "GCP_PROJECT",
                "GOROOT",
                "SPARK_HOME",
                "PYSPARK_PYTHON",
                "PYSPARK_DRIVER_PYTHON",
            }
        }
        env.update(
            {
                "GOOGLE_CLOUD_PROJECT": project,
                "LOCALCLOUD_GATEWAY_URL": str(gateway),
                "LOCALCLOUD_TEST": "1",
            }
        )
        for variable in manifest["endpoint_variables"]:
            if not advertised.get(variable):
                raise RuntimeError("LocalCloud did not advertise " + variable)
            env[variable] = mapped_endpoint(str(advertised[variable]))
        print(
            "LocalCloud:",
            gateway,
            "project:",
            project,
            "services:",
            ", ".join(manifest["services"]),
            flush=True,
        )
        phases = [] if args.skip_build else manifest["build"]
        phases = [
            *phases,
            manifest["full_suite"] if args.full_suite else manifest["dedicated_lane"],
        ]
        for phase in phases:
            cwd = (ROOT / phase["cwd"]).resolve()
            if not cwd.is_relative_to(ROOT):
                raise RuntimeError("Test working directory escapes the repository")
            print("Running:", " ".join(phase["command"]), flush=True)
            result = subprocess.run(phase["command"], cwd=cwd, env=env, check=False)
            if result.returncode:
                return result.returncode
        failed = False
        return 0
    finally:
        try:
            if project_owned:
                request("DELETE", "/projects/" + project)
        finally:
            if container_created:
                logs = docker("logs", "--tail", "100", ident, check=False)
                if failed:
                    print(logs, file=sys.stderr)
                try:
                    docker("rm", "-f", ident)
                finally:
                    if volume_created:
                        docker("volume", "rm", ident)
            elif volume_created:
                docker("volume", "rm", ident)


if __name__ == "__main__":
    sys.exit(main())
