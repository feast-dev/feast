import pkg_resources


def get_version():
    """Returns version information of the Feast Python Package."""
    try:
        sdk_version = pkg_resources.get_distribution("feast").version
    except pkg_resources.DistributionNotFound:
        sdk_version = "unknown"
    return sdk_version


def get_clean_version():
    """Returns a cleaned version information of the Feast Python Package."""
    version = get_version()
    return version.replace(".", "_").replace("+", "_")
