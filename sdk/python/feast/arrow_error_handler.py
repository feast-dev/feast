import json
import logging
import time
from functools import wraps

import pyarrow.flight as fl

from feast.errors import FeastError

logger = logging.getLogger(__name__)

BACKOFF_FACTOR = 0.5


def arrow_client_error_handling_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Retry only applies to FeastFlightClient methods where args[0] (self)
        # carries _connection_retries from RemoteOfflineStoreConfig.
        # Standalone stream functions (write_table, read_all) get 0 retries:
        # broken streams can't be reused and retrying risks duplicate writes.
        max_retries = max(0, getattr(args[0], "_connection_retries", 0)) if args else 0

        for attempt in range(max_retries + 1):
            try:
                return func(*args, **kwargs)
            except fl.FlightUnavailableError as e:
                if attempt < max_retries:
                    wait_time = BACKOFF_FACTOR * (2**attempt)
                    logger.warning(
                        "Transient Arrow Flight error on attempt %d/%d, "
                        "retrying in %.1fs: %s",
                        attempt + 1,
                        max_retries + 1,
                        wait_time,
                        e,
                    )
                    time.sleep(wait_time)
                    continue
                mapped_error = FeastError.from_error_detail(_get_exception_data(str(e)))
                if mapped_error is not None:
                    raise mapped_error
                raise e
            except Exception as e:
                mapped_error = FeastError.from_error_detail(
                    _get_exception_data(e.args[0])
                )
                if mapped_error is not None:
                    raise mapped_error
                raise e

    return wrapper


def arrow_server_error_handling_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if isinstance(e, FeastError):
                raise fl.FlightError(e.to_error_detail())
            # Re-raise non-Feast exceptions so Arrow Flight returns a proper error
            # instead of allowing the server method to return None.
            raise e

    return wrapper


def _get_exception_data(except_str) -> str:
    if not isinstance(except_str, str):
        return ""

    substring = "Flight error: "
    position = except_str.find(substring)
    if position == -1:
        return ""

    search_start = position + len(substring)
    json_start = except_str.find("{", search_start)
    if json_start == -1:
        return ""

    search_pos = json_start
    while True:
        json_end = except_str.find("}", search_pos + 1)
        if json_end == -1:
            return ""
        candidate = except_str[json_start : json_end + 1]
        try:
            json.loads(candidate)
            return candidate
        except (json.JSONDecodeError, ValueError):
            search_pos = json_end
