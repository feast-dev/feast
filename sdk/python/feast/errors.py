from typing import Any, List, Set

from colorama import Fore, Style

from feast.field import Field


class DataSourceNotFoundException(Exception):
    def __init__(self, path):
        super().__init__(
            f"Unable to find table at '{path}'. Please check that table exists."
        )


class DataSourceNoNameException(Exception):
    def __init__(self):
        super().__init__(
            "Unable to infer a name for this data source. Either table or name must be specified."
        )


class DataSourceRepeatNamesException(Exception):
    def __init__(self, ds_name: str):
        super().__init__(
            f"Multiple data sources share the same case-insensitive name {ds_name}."
        )


class FeastObjectNotFoundException(Exception):
    pass


class EntityNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(f"Entity {name} does not exist in project {project}")
        else:
            super().__init__(f"Entity {name} does not exist")


class FeatureServiceNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(
                f"Feature service {name} does not exist in project {project}"
            )
        else:
            super().__init__(f"Feature service {name} does not exist")


class FeatureViewNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(f"Feature view {name} does not exist in project {project}")
        else:
            super().__init__(f"Feature view {name} does not exist")


class OnDemandFeatureViewNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(
                f"On demand feature view {name} does not exist in project {project}"
            )
        else:
            super().__init__(f"On demand feature view {name} does not exist")


class RequestDataNotFoundInEntityDfException(FeastObjectNotFoundException):
    def __init__(self, feature_name, feature_view_name):
        super().__init__(
            f"Feature {feature_name} not found in the entity dataframe, but required by feature view {feature_view_name}"
        )


class RequestDataNotFoundInEntityRowsException(FeastObjectNotFoundException):
    def __init__(self, feature_names):
        super().__init__(
            f"Required request data source features {feature_names} not found in the entity rows, but required by feature views"
        )


class DataSourceObjectNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(f"Data source {name} does not exist in project {project}")
        else:
            super().__init__(f"Data source {name} does not exist")


class S3RegistryBucketNotExist(FeastObjectNotFoundException):
    def __init__(self, bucket):
        super().__init__(f"S3 bucket {bucket} for the Feast registry does not exist")


class S3RegistryBucketForbiddenAccess(FeastObjectNotFoundException):
    def __init__(self, bucket):
        super().__init__(f"S3 bucket {bucket} for the Feast registry can't be accessed")


class SavedDatasetNotFound(FeastObjectNotFoundException):
    def __init__(self, name: str, project: str):
        super().__init__(f"Saved dataset {name} does not exist in project {project}")


class ValidationReferenceNotFound(FeastObjectNotFoundException):
    def __init__(self, name: str, project: str):
        super().__init__(
            f"Validation reference {name} does not exist in project {project}"
        )


class FeastProviderLoginError(Exception):
    """Error class that indicates a user has not authenticated with their provider."""


class FeastProviderNotImplementedError(Exception):
    def __init__(self, provider_name):
        super().__init__(f"Provider '{provider_name}' is not implemented")


class FeastRegistryNotSetError(Exception):
    def __init__(self):
        super().__init__("Registry is not set, but is required")


class FeastFeatureServerTypeInvalidError(Exception):
    def __init__(self, feature_server_type: str):
        super().__init__(
            f"Feature server type was set to {feature_server_type}, but this type is invalid"
        )


class FeastRegistryTypeInvalidError(Exception):
    def __init__(self, registry_type: str):
        super().__init__(
            f"Feature server type was set to {registry_type}, but this type is invalid"
        )


class FeastModuleImportError(Exception):
    def __init__(self, module_name: str, class_name: str):
        super().__init__(
            f"Could not import module '{module_name}' while attempting to load class '{class_name}'"
        )


class FeastClassImportError(Exception):
    def __init__(self, module_name: str, class_name: str):
        super().__init__(
            f"Could not import class '{class_name}' from module '{module_name}'"
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
    def __init__(self, feature_refs_collisions: List[str], full_feature_names: bool):
        if full_feature_names:
            collisions = [ref.replace(":", "__") for ref in feature_refs_collisions]
            error_message = (
                "To resolve this collision, please ensure that the feature views or their own features "
                "have different names. If you're intentionally joining the same feature view twice on "
                "different sets of entities, please rename one of the feature views with '.with_name'."
            )
        else:
            collisions = [ref.split(":")[1] for ref in feature_refs_collisions]
            error_message = (
                "To resolve this collision, either use the full feature name by setting "
                "'full_feature_names=True', or ensure that the features in question have different names."
            )

        feature_names = ", ".join(set(collisions))
        super().__init__(
            f"Duplicate features named {feature_names} found.\n{error_message}"
        )


class SpecifiedFeaturesNotPresentError(Exception):
    def __init__(
        self,
        specified_features: List[Field],
        inferred_features: List[Field],
        feature_view_name: str,
    ):
        super().__init__(
            f"Explicitly specified features {specified_features} not found in inferred list of features "
            f"{inferred_features} for '{feature_view_name}'"
        )


class SavedDatasetLocationAlreadyExists(Exception):
    def __init__(self, location: str):
        super().__init__(f"Saved dataset location {location} already exists.")


class FeastOfflineStoreInvalidName(Exception):
    def __init__(self, offline_store_class_name: str):
        super().__init__(
            f"Offline Store Class '{offline_store_class_name}' should end with the string `OfflineStore`.'"
        )


class FeastOnlineStoreInvalidName(Exception):
    def __init__(self, online_store_class_name: str):
        super().__init__(
            f"Online Store Class '{online_store_class_name}' should end with the string `OnlineStore`.'"
        )


class FeastInvalidAuthConfigClass(Exception):
    def __init__(self, auth_config_class_name: str):
        super().__init__(
            f"Auth Config Class '{auth_config_class_name}' should end with the string `AuthConfig`.'"
        )


class FeastInvalidBaseClass(Exception):
    def __init__(self, class_name: str, class_type: str):
        super().__init__(
            f"Class '{class_name}' should have `{class_type}` as a base class."
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


class DockerDaemonNotRunning(Exception):
    def __init__(self):
        super().__init__(
            "The Docker Python sdk cannot connect to the Docker daemon. Please make sure you have"
            "the docker daemon installed, and that it is running."
        )


class RegistryInferenceFailure(Exception):
    def __init__(self, repo_obj_type: str, specific_issue: str):
        super().__init__(
            f"Inference to fill in missing information for {repo_obj_type} failed. {specific_issue}. "
            "Try filling the information explicitly."
        )


class BigQueryJobStillRunning(Exception):
    def __init__(self, job_id):
        super().__init__(f"The BigQuery job with ID '{job_id}' is still running.")


class BigQueryJobCancelled(Exception):
    def __init__(self, job_id):
        super().__init__(f"The BigQuery job with ID '{job_id}' was cancelled")


class RedshiftCredentialsError(Exception):
    def __init__(self):
        super().__init__("Redshift API failed due to incorrect credentials")


class RedshiftQueryError(Exception):
    def __init__(self, details):
        super().__init__(f"Redshift SQL Query failed to finish. Details: {details}")


class RedshiftTableNameTooLong(Exception):
    def __init__(self, table_name: str):
        super().__init__(
            f"Redshift table names have a maximum length of 127 characters, but the table name {table_name} has length {len(table_name)} characters."
        )


class SnowflakeCredentialsError(Exception):
    def __init__(self):
        super().__init__("Snowflake Connector failed due to incorrect credentials")


class SnowflakeQueryError(Exception):
    def __init__(self, details):
        super().__init__(f"Snowflake SQL Query failed to finish. Details: {details}")


class EntityTimestampInferenceException(Exception):
    def __init__(self, expected_column_name: str):
        super().__init__(
            f"Please provide an entity_df with a column named {expected_column_name} representing the time of events."
        )


class FeatureViewMissingDuringFeatureServiceInference(Exception):
    def __init__(self, feature_view_name: str, feature_service_name: str):
        super().__init__(
            f"Missing {feature_view_name} feature view during inference for {feature_service_name} feature service."
        )


class InvalidEntityType(Exception):
    def __init__(self, entity_type: type):
        super().__init__(
            f"The entity dataframe you have provided must be a Pandas DataFrame or a SQL query, "
            f"but we found: {entity_type} "
        )


class ConflictingFeatureViewNames(Exception):
    # TODO: print file location of conflicting feature views
    def __init__(self, feature_view_name: str):
        super().__init__(
            f"The feature view name: {feature_view_name} refers to feature views of different types."
        )


class FeastInvalidInfraObjectType(Exception):
    def __init__(self):
        super().__init__("Could not identify the type of the InfraObject.")


class SnowflakeIncompleteConfig(Exception):
    def __init__(self, e: KeyError):
        super().__init__(f"{e} not defined in a config file or feature_store.yaml file")


class SnowflakeQueryUnknownError(Exception):
    def __init__(self, query: str):
        super().__init__(f"Snowflake query failed: {query}")


class InvalidFeaturesParameterType(Exception):
    def __init__(self, features: Any):
        super().__init__(
            f"Invalid `features` parameter type {type(features)}. Expected one of List[str] and FeatureService."
        )


class EntitySQLEmptyResults(Exception):
    def __init__(self, entity_sql: str):
        super().__init__(
            f"No entity values found from the specified SQL query to generate the entity dataframe: {entity_sql}."
        )


class EntityDFNotDateTime(Exception):
    def __init__(self):
        super().__init__(
            "The entity dataframe specified does not have the timestamp field as a datetime."
        )


class PushSourceNotFoundException(Exception):
    def __init__(self, push_source_name: str):
        super().__init__(f"Unable to find push source '{push_source_name}'.")


class ReadOnlyRegistryException(Exception):
    def __init__(self):
        super().__init__("Registry implementation is read-only.")


class DataFrameSerializationError(Exception):
    def __init__(self, input_dict: dict):
        super().__init__(
            f"Failed to serialize the provided dictionary into a pandas DataFrame: {input_dict.keys()}"
        )


class PermissionNotFoundException(Exception):
    def __init__(self, name, project):
        super().__init__(f"Permission {name} does not exist in project {project}")


class PermissionObjectNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(f"Permission {name} does not exist in project {project}")
        else:
            super().__init__(f"Permission {name} does not exist")


class ZeroRowsQueryResult(Exception):
    def __init__(self, query: str):
        super().__init__(f"This query returned zero rows:\n{query}")


class ZeroColumnQueryResult(Exception):
    def __init__(self, query: str):
        super().__init__(f"This query returned zero columns:\n{query}")
