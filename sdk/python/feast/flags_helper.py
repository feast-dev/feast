import os

ENV_FLAG_IS_TEST = "IS_TEST"


def _env_flag_enabled(name: str) -> bool:
    return os.getenv(name, default="False") == "True"


def is_test() -> bool:
    return _env_flag_enabled(ENV_FLAG_IS_TEST)
