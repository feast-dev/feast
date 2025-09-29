# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8 
# Pydantic Version: 2.10.6 
from enum import IntEnum
from google.protobuf.message import Message  # type: ignore
from protobuf_to_pydantic.customer_validator import check_one_of
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator
import typing


class Store(BaseModel):
    """
     Store provides a location where Feast reads and writes feature values.
 Feature values will be written to the Store in the form of FeatureRow elements.
 The way FeatureRow is encoded and decoded when it is written to and read from
 the Store depends on the type of the Store.

    """
    class RedisConfig(BaseModel):
        host: str = Field(default="")
        port: int = Field(default=0)
# Optional. The number of milliseconds to wait before retrying failed Redis connection.
# By default, Feast uses exponential backoff policy and "initial_backoff_ms" sets the initial wait duration.
        initial_backoff_ms: int = Field(default=0)
# Optional. Maximum total number of retries for connecting to Redis. Default to zero retries.
        max_retries: int = Field(default=0)
# Optional. How often flush data to redis
        flush_frequency_seconds: int = Field(default=0)
# Optional. Connect over SSL.
        ssl: bool = Field(default=False)

    class RedisClusterConfig(BaseModel):
        class ReadFrom(IntEnum):
            """
             Optional. Priority of nodes when reading from cluster
            """
            MASTER = 0
            MASTER_PREFERRED = 1
            REPLICA = 2
            REPLICA_PREFERRED = 3

        model_config = ConfigDict(validate_default=True)
# List of Redis Uri for all the nodes in Redis Cluster, comma separated. Eg. host1:6379, host2:6379
        connection_string: str = Field(default="")
        initial_backoff_ms: int = Field(default=0)
        max_retries: int = Field(default=0)
# Optional. How often flush data to redis
        flush_frequency_seconds: int = Field(default=0)
# Optional. Append a prefix to the Redis Key
        key_prefix: str = Field(default="")
# Optional. Enable fallback to another key prefix if the original key is not present.
# Useful for migrating key prefix without re-ingestion. Disabled by default.
        enable_fallback: bool = Field(default=False)
# Optional. This would be the fallback prefix to use if enable_fallback is true.
        fallback_prefix: str = Field(default="")
        read_from: ReadFrom = Field(default=0)

    class Subscription(BaseModel):
# Name of project that the feature sets belongs to. This can be one of
# - [project_name]
# - *
# If an asterisk is provided, filtering on projects will be disabled. All projects will
# be matched. It is NOT possible to provide an asterisk with a string in order to do
# pattern matching.
        project: str = Field(default="")
# Name of the desired feature set. Asterisks can be used as wildcards in the name.
# Matching on names is only permitted if a specific project is defined. It is disallowed
# If the project name is set to "*"
# e.g.
# - * can be used to match all feature sets
# - my-feature-set* can be used to match all features prefixed by "my-feature-set"
# - my-feature-set-6 can be used to select a single feature set
        name: str = Field(default="")
# All matches with exclude enabled will be filtered out instead of added
        exclude: bool = Field(default=False)

    class StoreType(IntEnum):
        INVALID = 0
        REDIS = 1
        REDIS_CLUSTER = 4

    _one_of_dict = {"Store.config": {"fields": {"redis_cluster_config", "redis_config"}}}
    one_of_validator = model_validator(mode="before")(check_one_of)
    model_config = ConfigDict(validate_default=True)
# Name of the store.
    name: str = Field(default="")
# Type of store.
    type: "Store.StoreType" = Field(default=0)
# Feature sets to subscribe to.
    subscriptions: typing.List["Store.Subscription"] = Field(default_factory=list)
    redis_config: "Store.RedisConfig" = Field(default_factory=lambda : Store.RedisConfig())
    redis_cluster_config: "Store.RedisClusterConfig" = Field(default_factory=lambda : Store.RedisClusterConfig())
