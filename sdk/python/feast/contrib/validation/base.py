import io

try:
    from pyspark import cloudpickle
except ImportError:
    raise ImportError("pyspark must be installed to enable validation functionality")


def serialize_udf(fun, return_type) -> bytes:
    buffer = io.BytesIO()
    command = (fun, return_type)
    cloudpickle.dump(command, buffer)
    return buffer.getvalue()
