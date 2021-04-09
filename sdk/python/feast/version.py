import pkg_resources


def get_version():
    """
    Returns version information of the Feast Python Package
    """

    try:
        sdk_version = pkg_resources.get_distribution("feast").version
    except pkg_resources.DistributionNotFound:
        sdk_version = "unknown"
    return sdk_version
