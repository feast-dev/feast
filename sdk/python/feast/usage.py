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
import contextlib
import contextvars
import dataclasses
import hashlib
import logging
import os
import platform
import sys
import typing
import uuid
from datetime import datetime
from functools import wraps
from os.path import expanduser, join
from pathlib import Path

import requests

from feast.constants import DEFAULT_FEAST_USAGE_VALUE, FEAST_USAGE
from feast.version import get_version

USAGE_ENDPOINT = "https://usage.feast.dev"

_logger = logging.getLogger(__name__)
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

_is_enabled = os.getenv(FEAST_USAGE, default=DEFAULT_FEAST_USAGE_VALUE) == "True"

_constant_attributes = {
    "session_id": str(uuid.uuid4()),
    "installation_id": None,
    "version": get_version(),
    "python_version": platform.python_version(),
    "platform": platform.platform(),
    "env_signature": hashlib.md5(
        ",".join(
            sorted([k for k in os.environ.keys() if not k.startswith("FEAST")])
        ).encode()
    ).hexdigest(),
}


@dataclasses.dataclass
class FnCall:
    fn_name: str
    id: str

    start: datetime
    end: typing.Optional[datetime] = None

    parent_id: typing.Optional[str] = None


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
        assert 0 < ratio <= 1, "Ratio must be within (0, 1]"
        self.ratio = ratio
        self.total_counter = 0
        self.sampled_counter = 0

    def should_record(self, event) -> bool:
        self.total_counter += 1
        if self.total_counter == self.MAX_COUNTER:
            self.total_counter = 1
            self.sampled_counter = 1

        decision = self.sampled_counter < self.ratio * self.total_counter
        self.sampled_counter += int(decision)
        return decision

    @property
    def priority(self):
        return int(1 / self.ratio)


class UsageContext:
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


_context = contextvars.ContextVar("usage_context", default=UsageContext())


def _set_installation_id():
    if os.getenv("FEAST_FORCE_USAGE_UUID"):
        _constant_attributes["installation_id"] = os.getenv("FEAST_FORCE_USAGE_UUID")
        _constant_attributes["installation_ts"] = datetime.utcnow().isoformat()
        return

    feast_home_dir = join(expanduser("~"), ".feast")
    installation_timestamp = datetime.utcnow()

    try:
        Path(feast_home_dir).mkdir(exist_ok=True)
        usage_filepath = join(feast_home_dir, "usage")

        if os.path.exists(usage_filepath):
            installation_timestamp = datetime.utcfromtimestamp(
                os.path.getmtime(usage_filepath)
            )
            with open(usage_filepath, "r") as f:
                installation_id = f.read()
        else:
            installation_id = str(uuid.uuid4())

            with open(usage_filepath, "w") as f:
                f.write(installation_id)
            print(
                "Feast is an open source project that collects "
                "anonymized error reporting and usage statistics. To opt out or learn"
                " more see https://docs.feast.dev/reference/usage"
            )
    except OSError as e:
        _logger.debug(f"Unable to configure usage {e}")
        installation_id = "undefined"

    _constant_attributes["installation_id"] = installation_id
    _constant_attributes["installation_ts"] = installation_timestamp.isoformat()


_set_installation_id()


def _export(event: typing.Dict[str, typing.Any]):
    _executor.submit(requests.post, USAGE_ENDPOINT, json=event, timeout=30)


def _produce_event(ctx: UsageContext):
    is_test = bool({"pytest", "unittest"} & sys.modules.keys())
    event = {
        "timestamp": datetime.utcnow().isoformat(),
        "is_test": is_test,
        "is_webserver": (
            not is_test and bool({"uwsgi", "gunicorn", "fastapi"} & sys.modules.keys())
        ),
        "calls": [
            dict(
                fn_name=c.fn_name,
                id=c.id,
                parent_id=c.parent_id,
                start=c.start and c.start.isoformat(),
                end=c.end and c.end.isoformat(),
            )
            for c in reversed(ctx.completed_calls)
        ],
        "entrypoint": ctx.completed_calls[-1].fn_name,
        "exception": repr(ctx.exception) if ctx.exception else None,
        "traceback": ctx.traceback if ctx.exception else None,
        **_constant_attributes,
    }
    event.update(ctx.attributes)

    if ctx.sampler and not ctx.sampler.should_record(event):
        return

    _export(event)


@contextlib.contextmanager
def tracing_span(name):
    """
    Context manager for wrapping heavy parts of code in tracing span
    """
    if _is_enabled:
        ctx = _context.get()
        if not ctx.call_stack:
            raise RuntimeError("tracing_span must be called in usage context")

        last_call = ctx.call_stack[-1]
        fn_call = FnCall(
            id=uuid.uuid4().hex,
            parent_id=last_call.id,
            fn_name=f"{last_call.fn_name}.{name}",
            start=datetime.utcnow(),
        )
    try:
        yield
    finally:
        if _is_enabled:
            fn_call.end = datetime.utcnow()
            ctx.completed_calls.append(fn_call)


def log_exceptions_and_usage(*args, **attrs):
    """
        This function decorator enables three components:
        1. Error tracking
        2. Usage statistic collection
        3. Time profiling

        This data is being collected, anonymized and sent to Feast Developers.
        All events from nested decorated functions are being grouped into single event
        to build comprehensive context useful for profiling and error tracking.

        Usage example (will result in one output event):
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
        if not _is_enabled:
            return func

        @wraps(func)
        def wrapper(*args, **kwargs):
            ctx = _context.get()
            ctx.call_stack.append(
                FnCall(
                    id=uuid.uuid4().hex,
                    parent_id=ctx.call_stack[-1].id if ctx.call_stack else None,
                    fn_name=_fn_fullname(func),
                    start=datetime.utcnow(),
                )
            )
            ctx.attributes.update(attrs)

            try:
                return func(*args, **kwargs)
            except Exception:
                if ctx.exception:
                    # exception was already recorded
                    raise

                _, exc, traceback = sys.exc_info()
                ctx.exception = exc
                ctx.traceback = _trace_to_log(traceback)

                if traceback:
                    raise exc.with_traceback(traceback)

                raise exc
            finally:
                last_call = ctx.call_stack.pop(-1)
                last_call.end = datetime.utcnow()
                ctx.completed_calls.append(last_call)
                ctx.sampler = (
                    sampler if sampler.priority > ctx.sampler.priority else ctx.sampler
                )

                if not ctx.call_stack:
                    # we reached the root of the stack
                    _context.set(UsageContext())  # reset context to default values
                    _produce_event(ctx)

        return wrapper

    if args:
        return decorator(args[0])

    return decorator


def log_exceptions(*args, **attrs):
    """
    Function decorator that track errors and send them to Feast Developers
    """

    def decorator(func):
        if not _is_enabled:
            return func

        @wraps(func)
        def wrapper(*args, **kwargs):
            if _context.get().call_stack:
                # we're already inside usage context
                # let it handle exception
                return func(*args, **kwargs)

            fn_call = FnCall(
                id=uuid.uuid4().hex, fn_name=_fn_fullname(func), start=datetime.utcnow()
            )
            try:
                return func(*args, **kwargs)
            except Exception:
                _, exc, traceback = sys.exc_info()

                fn_call.end = datetime.utcnow()

                ctx = UsageContext()
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
    """
    Extend current context with custom attribute
    """
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
