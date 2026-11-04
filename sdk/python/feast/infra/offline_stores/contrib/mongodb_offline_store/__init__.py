import feast.version

try:
    from pymongo.driver_info import DriverInfo

    DRIVER_METADATA = DriverInfo(name="Feast", version=feast.version.get_version())
except ImportError:
    DRIVER_METADATA = None  # type: ignore[assignment]
