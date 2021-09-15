import os

FLAG_ALPHA_FEATURES_NAME = "ENABLE_ALPHA_FEATURES"
FLAG_ON_DEMAND_TRANSFORM_NAME = "ENABLE_ON_DEMAND_TRANSFORMS"
FLAG_PYTHON_FEATURE_SERVER_NAME = "ENABLE_PYTHON_FEATURE_SERVER"
FLAG_IS_TEST = "IS_TEST"
FLAG_ENABLE_USAGE = "FEAST_USAGE"


def _flag_enabled(name: str, enable_by_default: bool = False) -> bool:
    return os.getenv(name, default=str(enable_by_default)) == "True"


def _experimental_features_enabled() -> bool:
    return _flag_enabled(FLAG_ALPHA_FEATURES_NAME)


def enable_usage() -> bool:
    return _flag_enabled(FLAG_ENABLE_USAGE)


def is_test() -> bool:
    return _flag_enabled(FLAG_IS_TEST)


def enable_on_demand_feature_views() -> bool:
    if is_test():
        return True
    return _experimental_features_enabled() and _flag_enabled(
        FLAG_ON_DEMAND_TRANSFORM_NAME, enable_by_default=True
    )


def enable_python_feature_server() -> bool:
    if is_test():
        return True
    return _experimental_features_enabled() and _flag_enabled(
        FLAG_PYTHON_FEATURE_SERVER_NAME, enable_by_default=True
    )
