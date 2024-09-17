import logging
from functools import wraps

import pyarrow.flight as fl

from feast.errors import FeastError

logger = logging.getLogger(__name__)


def arrow_client_error_handling_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            mapped_error = FeastError.from_error_detail(_get_exception_data(e.args[0]))
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
