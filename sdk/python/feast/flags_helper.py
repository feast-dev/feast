import os

from feast import flags
from feast.repo_config import RepoConfig


def _env_flag_enabled(name: str) -> bool:
    return os.getenv(name, default="False") == "True"


def feature_flag_enabled(repo_config: RepoConfig, flag_name: str) -> bool:
    if is_test():
        return True
    return (
        _alpha_feature_flag_enabled(repo_config)
        and repo_config.flags is not None
        and flag_name in repo_config.flags
        and repo_config.flags[flag_name]
    )


def _alpha_feature_flag_enabled(repo_config: RepoConfig) -> bool:
    return (
        repo_config.flags is not None
        and flags.FLAG_ALPHA_FEATURES_NAME in repo_config.flags
        and repo_config.flags[flags.FLAG_ALPHA_FEATURES_NAME]
    )


def is_test() -> bool:
    return _env_flag_enabled(flags.ENV_FLAG_IS_TEST)


def enable_on_demand_feature_views(repo_config: RepoConfig) -> bool:
    return feature_flag_enabled(repo_config, flags.FLAG_ON_DEMAND_TRANSFORM_NAME)


def enable_python_feature_server(repo_config: RepoConfig) -> bool:
    return feature_flag_enabled(repo_config, flags.FLAG_PYTHON_FEATURE_SERVER_NAME)
