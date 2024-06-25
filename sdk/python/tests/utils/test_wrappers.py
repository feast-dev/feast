import warnings


def no_warnings(func):
    def wrapper_no_warnings(*args, **kwargs):
        with warnings.catch_warnings(record=True) as record:
            func(*args, **kwargs)

        if len(record) > 0:
            raise AssertionError(
                "Warnings were raised: " + ", ".join([str(w) for w in record])
            )

    return wrapper_no_warnings
