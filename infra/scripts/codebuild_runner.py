#!/usr/bin/env python

# This is a thin wrapper for AWS Codebuild API to kick off a build, wait for it to finish,
# and tail build logs while it is running.

import os
import json
from typing import Dict, Any, List, Optional, AsyncGenerator
from datetime import datetime
import asyncio
import sys
import argparse
import boto3
from botocore.config import Config


class LogTailer:
    """ A simple cloudwatch log tailer. """

    _next_token: Optional[str]

    def __init__(self, client, log_group: str, log_stream: str):
        self._client = client
        self._next_token = None
        self._log_group = log_group
        self._log_stream = log_stream

    def _get_log_events_args(self) -> Dict[str, Any]:
        res = dict(
            logGroupName=self._log_group,
            logStreamName=self._log_stream,
            limit=100,
            startFromHead=True,
        )
        if self._next_token:
            res["nextToken"] = self._next_token
        return res

    async def tail_chunk(self) -> List[Dict[str, str]]:
        max_sleep = 5.0
        SLEEP_TIME = 0.5

        while max_sleep > 0:
            resp = self._client.get_log_events(**self._get_log_events_args())
            events = resp["events"]
            self._next_token = resp.get("nextForwardToken")
            if events:
                return events
            else:
                max_sleep -= SLEEP_TIME
                await asyncio.sleep(SLEEP_TIME)
        else:
            return []

    async def read_all_chunks(self) -> AsyncGenerator[List[Dict[str, str]], None]:
        while True:
            resp = self._client.get_log_events(**self._get_log_events_args())
            events = resp["events"]
            self._next_token = resp.get("nextForwardToken")
            if events:
                yield events
            else:
                return


async def _wait_build_state(
    client, build_id, desired_phase: Optional[str], desired_states: List[str]
) -> Dict[str, Any]:
    """ Wait until the build is in one of the desired states, or in the desired phase. """
    while True:
        resp = client.batch_get_builds(ids=[build_id])
        assert len(resp["builds"]) == 1
        build = resp["builds"][0]
        if build["buildStatus"] in desired_states:
            return build
        for phase in build["phases"]:
            if desired_phase and (phase["phaseType"] == desired_phase):
                return build

        await asyncio.sleep(2)


def print_log_event(event) -> None:
    print(
        str(datetime.fromtimestamp(event["timestamp"] / 1000.0)),
        event["message"],
        end="",
    )


async def main() -> None:
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument(
        "--project-name", default="feast-ci-project", type=str, help="Project name"
    )
    parser.add_argument(
        "--source-location",
        type=str,
        help="Source location, e.g. https://github.com/feast/feast.git",
    )
    parser.add_argument(
        "--source-version", type=str, help="Source version, e.g. master"
    )
    parser.add_argument(
        "--location-from-prow", action='store_true', help="Infer source location and version from prow environment variables"
    )
    args = parser.parse_args()

    if args.location_from_prow:
        job_spec = json.loads(os.getenv('JOB_SPEC', ''))
        source_location = job_spec['refs']['repo_link']
        source_version = source_version_from_prow_job_spec(job_spec)
    else:
        source_location = args.source_location
        source_version = args.source_version

    await run_build(
        project_name=args.project_name,
        source_location=source_location,
        source_version=source_version,
    )

def source_version_from_prow_job_spec(job_spec: Dict[str, Any]) -> str:
    pull = job_spec['refs']['pulls'][0]
    return f'refs/pull/{pull["number"]}/head^{{{pull["sha"]}}}'

async def run_build(project_name: str, source_version: str, source_location: str):
    print(f"Building {project_name} at {source_version}", file=sys.stderr)

    config = Config(
        retries = {
            'max_attempts': 10,
        }
    )

    logs_client = boto3.client("logs", region_name="us-west-2", config=config)
    codebuild_client = boto3.client("codebuild", region_name="us-west-2")

    print("Submitting the build..", file=sys.stderr)
    build_resp = codebuild_client.start_build(
        projectName=project_name,
        sourceLocationOverride=source_location,
        sourceVersion=source_version,
    )

    build_id = build_resp["build"]["id"]

    try:
        print(
            "Waiting for the INSTALL phase to start before tailing the log",
            file=sys.stderr,
        )
        build = await _wait_build_state(
            codebuild_client,
            build_id,
            desired_phase="INSTALL",
            desired_states=["SUCCEEDED", "FAILED", "STOPPED", "TIMED_OUT", "FAULT"],
        )

        if build["buildStatus"] != "IN_PROGRESS":
            print(
                f"Build failed before install phase: {build['buildStatus']}",
                file=sys.stderr,
            )
            sys.exit(1)

        log_tailer = LogTailer(
            logs_client,
            log_stream=build["logs"]["streamName"],
            log_group=build["logs"]["groupName"],
        )

        waiter_task = asyncio.get_event_loop().create_task(
            _wait_build_state(
                codebuild_client,
                build_id,
                desired_phase=None,
                desired_states=["SUCCEEDED", "FAILED", "STOPPED", "TIMED_OUT", "FAULT"],
            )
        )

        while not waiter_task.done():
            events = await log_tailer.tail_chunk()
            for event in events:
                print_log_event(event)

        build_status = waiter_task.result()["buildStatus"]
        if build_status == "SUCCEEDED":
            print(f"Build {build_status}", file=sys.stderr)
        else:
            print(f"Build {build_status}", file=sys.stderr)
            sys.exit(1)
    except KeyboardInterrupt:
        print(f"Stopping build {build_id}", file=sys.stderr)
        codebuild_client.stop_build(id=build_id)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
