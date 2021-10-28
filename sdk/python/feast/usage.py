# Copyright 2019 The Feast Authors
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
import concurrent.futures
import contextvars
import dataclasses
import enum
import logging
import os
import random
import sys
import typing
import uuid
from collections import defaultdict
from datetime import datetime
from functools import wraps
from os.path import expanduser, join
from pathlib import Path

import requests

from feast.constants import FEAST_USAGE
from feast.version import get_version

USAGE_ENDPOINT = "https://usage.feast.dev"
_logger = logging.getLogger(__name__)

executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)


@dataclasses.dataclass
class FnCall:
    fn_name: str

    start: datetime
    end: typing.Optional[datetime] = None


class Sampler:
    def should_record(self, event) -> bool:
        raise NotImplementedError

    @property
    def priority(self):
        return 0


class AlwaysSampler(Sampler):
    def should_record(self, event) -> bool:
        return True


class RatioSampler(Sampler):
    MAX_COUNTER = (1 << 32) - 1

    def __init__(self, ratio):
        self.ratio = ratio
        self.counter = 0

    def should_record(self, event) -> bool:
        self.counter += 1
        if self.counter == self.MAX_COUNTER:
            self.counter = 1

        return self.counter < self.ratio * self.MAX_COUNTER

    @property
    def priority(self):
        return int(1 / self.ratio)


class TelemetryContext:
    attributes: typing.Dict[str, typing.Any]

    call_stack: typing.List[FnCall]
    completed_calls: typing.List[FnCall]

    exception: typing.Optional[Exception] = None
    traceback: typing.Optional[typing.Tuple[str, int, str]] = None

    sampler: Sampler = AlwaysSampler()

    def __init__(self):
        self.attributes = {}
        self.call_stack = []
        self.completed_calls = []


_context = contextvars.ContextVar("telemetry_context", default=TelemetryContext())
_session_id = str(uuid.uuid4())


def _is_enabled():
    return os.getenv(FEAST_USAGE, default="True") == "True"


def _installation_id():
    if os.getenv("FEAST_FORCE_USAGE_UUID"):
        return os.getenv("FEAST_FORCE_USAGE_UUID")

    feast_home_dir = join(expanduser("~"), ".feast")

    try:
        Path(feast_home_dir).mkdir(exist_ok=True)
        usage_filepath = join(feast_home_dir, "usage")

        if os.path.exists(usage_filepath):
            with open(usage_filepath, "r") as f:
                installation_id = f.read()
        else:
            installation_id = str(uuid.uuid4())

            with open(usage_filepath, "w") as f:
                f.write(installation_id)
            print(
                "Feast is an open source project that collects anonymized error reporting and usage statistics. To opt out or learn"
                " more see https://docs.feast.dev/reference/usage"
            )
    except OSError as e:
        _logger.debug(f"Unable to configure usage {e}")
        return "undefined"

    return installation_id


def _export(event: typing.Dict[str, typing.Any]):
    executor.submit(requests.post, USAGE_ENDPOINT, json=event)


def _produce_event(ctx: TelemetryContext):
    event = {
        "installation_id": _installation_id(),
        "session_id": _session_id,
        "timestamp": datetime.utcnow().isoformat(),
        "version": get_version(),
        "python_version": _get_python_version(),
        "os": sys.platform,
        "is_test": bool({"pytest", "unittest"} & sys.modules.keys()),
        "is_webserver": (
            not bool({"pytest", "unittest"} & sys.modules.keys())
            and bool({"uwsgi", "gunicorn", "fastapi"} & sys.modules.keys())
        ),
        "calls": [dataclasses.asdict(c) for c in reversed(ctx.completed_calls)],
        "entrypoint": ctx.completed_calls[-1].fn_name,
        "exception": repr(ctx.exception) if ctx.exception else None,
        "traceback": ctx.traceback if ctx.exception else None,
    }
    event.update(ctx.attributes)

    if ctx.sampler and not ctx.sampler.should_record(event):
        return

    _export(event)


def log_exceptions_and_usage(*args, **attrs):
    """
        Example:
            @log_exceptions_and_usage
            def fn(...):
                nested()

            @log_exceptions_and_usage(attr='value')
            def nested(...):
                deeply_nested()

            @log_exceptions_and_usage(attr2='value2', sample=RateSampler(rate=0.1))
            def deeply_nested(...):
                ...
    """
    sampler = attrs.pop("sampler", AlwaysSampler())

    def decorator(func):
        if not _is_enabled():
            return func

        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                ctx = _context.get()
                ctx.call_stack.append(
                    FnCall(fn_name=_fn_fullname(func), start=datetime.now())
                )
                ctx.attributes.update(attrs)
                _context.set(ctx)

                return func(*args, **kwargs)
            except Exception:
                _, exc, traceback = sys.exc_info()

                ctx = _context.get()
                if not ctx.exception:
                    ctx.exception = exc
                    ctx.traceback = _trace_to_log(traceback)
                    _context.set(ctx)

                if traceback:
                    raise exc.with_traceback(traceback)

                raise exc
            finally:
                ctx = _context.get()
                last_call = ctx.call_stack.pop(-1)
                last_call.end = datetime.now()
                ctx.completed_calls.append(last_call)
                ctx.sampler = (
                    sampler if sampler.priority > ctx.sampler.priority else ctx.sampler
                )

                if not ctx.call_stack:
                    # we reached the root of our context
                    _context.set(TelemetryContext())  # reset
                    _produce_event(ctx)
                else:
                    _context.set(ctx)

        return wrapper

    if args:
        return decorator(args[0])

    return decorator


def log_exceptions(*args, **attrs):
    def decorator(func):
        if not _is_enabled():
            return func

        @wraps(func)
        def wrapper(*args, **kwargs):
            if _context.get().call_stack:
                # we're already inside telemetry context
                # let it handle exception
                return func(*args, **kwargs)

            fn_call = FnCall(fn_name=_fn_fullname(func), start=datetime.now())
            try:
                return func(*args, **kwargs)
            except Exception as e:
                _, exc, traceback = sys.exc_info()

                fn_call.end = datetime.now()

                ctx = TelemetryContext()
                ctx.exception = exc
                ctx.traceback = _trace_to_log(traceback)
                ctx.attributes = attrs
                ctx.completed_calls.append(fn_call)
                _produce_event(ctx)

                if traceback:
                    raise exc.with_traceback(traceback)

                raise exc

        return wrapper

    if args:
        return decorator(args[0])

    return decorator


def set_usage_attribute(name, value):
    ctx = _context.get()
    ctx.attributes[name] = value


def _trim_filename(filename: str) -> str:
    return filename.split("/")[-1]


def _fn_fullname(fn: typing.Callable):
    return fn.__module__ + "." + fn.__qualname__


def _trace_to_log(traceback):
    log = []
    while traceback is not None:
        log.append(
            (
                _trim_filename(traceback.tb_frame.f_code.co_filename),
                traceback.tb_lineno,
                traceback.tb_frame.f_code.co_name,
            )
        )
        traceback = traceback.tb_next

    return log


def _get_python_version():
    return ".".join(map(str, sys.version_info))
