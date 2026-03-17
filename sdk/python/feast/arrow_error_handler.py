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
    substring = "Flight error: "

    # Find the starting index of the substring
    position = except_str.find(substring)
    end_json_index = except_str.find("}")

    if position != -1 and end_json_index != -1:
        # Extract the part of the string after the substring
        result = except_str[position + len(substring) : end_json_index + 1]
        return result

    return ""
