#!/usr/bin/env python3
"""Launch the Chronon service detached from the calling shell."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--chronon-dir", required=True)
    parser.add_argument("--java-bin", required=True)
    parser.add_argument("--service-jar", required=True)
    parser.add_argument("--service-port", required=True)
    parser.add_argument("--log-file", required=True)
    parser.add_argument("--pid-file", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    chronon_dir = Path(args.chronon_dir)
    log_path = Path(args.log_file)
    pid_path = Path(args.pid_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        args.java_bin,
        "-jar",
        args.service_jar,
        "run",
        "ai.chronon.service.WebServiceVerticle",
        f"-Dserver.port={args.service_port}",
        "-conf",
        "service/src/main/resources/example_config.json",
    ]

    with Path("/dev/null").open("rb") as devnull, log_path.open("wb") as log_file:
        process = subprocess.Popen(
            command,
            cwd=chronon_dir,
            stdin=devnull,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    pid_path.write_text(f"{process.pid}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
