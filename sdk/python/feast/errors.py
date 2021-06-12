from typing import Set

from colorama import Fore, Style


class FeastObjectNotFoundException(Exception):
    pass


class EntityNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(f"Entity {name} does not exist in project {project}")
        else:
            super().__init__(f"Entity {name} does not exist")


class FeatureViewNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(f"Feature view {name} does not exist in project {project}")
        else:
            super().__init__(f"Feature view {name} does not exist")


class FeatureTableNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(
                f"Feature table {name} does not exist in project {project}"
            )
        else:
            super().__init__(f"Feature table {name} does not exist")


class FeastProviderLoginError(Exception):
    """Error class that indicates a user has not authenticated with their provider."""


class FeastProviderNotImplementedError(Exception):
    def __init__(self, provider_name):
        super().__init__(f"Provider '{provider_name}' is not implemented")


class FeastProviderModuleImportError(Exception):
    def __init__(self, module_name):
        super().__init__(f"Could not import provider module '{module_name}'")


class FeastProviderClassImportError(Exception):
    def __init__(self, module_name, class_name):
        super().__init__(
            f"Could not import provider '{class_name}' from module '{module_name}'"
        )


class FeastExtrasDependencyImportError(Exception):
    def __init__(self, extras_type: str, nested_error: str):
        message = (
            nested_error
            + "\n"
            + f"You may need run {Style.BRIGHT + Fore.GREEN}pip install 'feast[{extras_type}]'{Style.RESET_ALL}"
        )
        super().__init__(message)


class FeastOfflineStoreUnsupportedDataSource(Exception):
    def __init__(self, offline_store_name: str, data_source_name: str):
        super().__init__(
            f"Offline Store '{offline_store_name}' does not support data source '{data_source_name}'"
        )


class FeatureNameCollisionError(Exception):
    def __init__(self, feature_name_collisions: str):
        super().__init__(
            f"The following feature name(s) have collisions: {feature_name_collisions}. Set 'feature_names_only' argument in the data retrieval function to False to use the full feature name which is prefixed by the feature view name."
        )


class FeastOnlineStoreUnsupportedDataSource(Exception):
    def __init__(self, online_store_name: str, data_source_name: str):
        super().__init__(
            f"Online Store '{online_store_name}' does not support data source '{data_source_name}'"
        )


class FeastEntityDFMissingColumnsError(Exception):
    def __init__(self, expected, missing):
        super().__init__(
            f"The entity dataframe you have provided must contain columns {expected}, "
            f"but {missing} were missing."
        )


class FeastJoinKeysDuringMaterialization(Exception):
    def __init__(
        self, source: str, join_key_columns: Set[str], source_columns: Set[str]
    ):
        super().__init__(
            f"The DataFrame from {source} being materialized must have at least {join_key_columns} columns present, "
            f"but these were missing: {join_key_columns - source_columns} "
        )
