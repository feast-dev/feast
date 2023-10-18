import pytest


def no_warnings(func):
    def wrapper_no_warnings(*args, **kwargs):
        with pytest.warns(None) as warnings:
            func(*args, **kwargs)

        if len(warnings) > 0:
            raise AssertionError(
                "Warnings were raised: " + ", ".join([str(w) for w in warnings])
            )

    return wrapper_no_warnings
