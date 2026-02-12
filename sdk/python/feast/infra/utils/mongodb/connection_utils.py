# Copyright 2025 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
MongoDB connection utilities for Feast.

This module provides helper functions for creating and managing MongoDB connections
for both the online and offline stores.
"""

import logging
from typing import Any, Union

try:
    from pymongo import AsyncMongoClient, MongoClient
    from pymongo.read_preferences import ReadPreference
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("mongodb", str(e))

logger = logging.getLogger(__name__)


def get_mongo_client(
    connection_string: str,
    max_pool_size: int = 50,
    min_pool_size: int = 10,
    write_concern_w: Union[int, str] = 1,
    read_preference: str = "primaryPreferred",
    **kwargs: Any,
) -> MongoClient:
    """
    Create a synchronous MongoDB client with connection pooling.

    Args:
        connection_string: MongoDB connection URI (mongodb:// or mongodb+srv://)
        max_pool_size: Maximum number of connections in the pool
        min_pool_size: Minimum number of connections in the pool
        write_concern_w: Write concern level (1, 'majority', etc.)
        read_preference: Read preference ('primary', 'primaryPreferred', 'secondary', etc.)
        **kwargs: Additional keyword arguments to pass to MongoClient

    Returns:
        MongoClient instance configured with connection pooling

    Raises:
        ConnectionError: If connection to MongoDB fails
    """
    read_pref = _parse_read_preference(read_preference)

    try:
        client = MongoClient(
            connection_string,
            maxPoolSize=max_pool_size,
            minPoolSize=min_pool_size,
            w=write_concern_w,
            readPreference=read_pref,
            **kwargs,
        )
        # Test the connection
        client.admin.command("ping")
        logger.info(
            f"Successfully connected to MongoDB (pool size: {min_pool_size}-{max_pool_size})"
        )
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise ConnectionError(f"Failed to connect to MongoDB: {e}") from e


def get_async_mongo_client(
    connection_string: str,
    max_pool_size: int = 50,
    min_pool_size: int = 10,
    write_concern_w: Union[int, str] = 1,
    read_preference: str = "primaryPreferred",
    **kwargs: Any,
) -> AsyncMongoClient:
    """
    Create an asynchronous MongoDB client with connection pooling.

    Args:
        connection_string: MongoDB connection URI (mongodb:// or mongodb+srv://)
        max_pool_size: Maximum number of connections in the pool
        min_pool_size: Minimum number of connections in the pool
        write_concern_w: Write concern level (1, 'majority', etc.)
        read_preference: Read preference ('primary', 'primaryPreferred', 'secondary', etc.)
        **kwargs: Additional keyword arguments to pass to AsyncMongoClient

    Returns:
        AsyncMongoClient instance configured with connection pooling

    Raises:
        ConnectionError: If connection to MongoDB fails
    """
    read_pref = _parse_read_preference(read_preference)

    try:
        client = AsyncMongoClient(
            connection_string,
            maxPoolSize=max_pool_size,
            minPoolSize=min_pool_size,
            w=write_concern_w,
            readPreference=read_pref,
            **kwargs,
        )
        logger.info(
            f"Async MongoDB client created (pool size: {min_pool_size}-{max_pool_size})"
        )
        return client
    except Exception as e:
        logger.error(f"Failed to create async MongoDB client: {e}")
        raise ConnectionError(f"Failed to create async MongoDB client: {e}") from e


def is_atlas(client: MongoClient) -> bool:
    """
    Detect if the MongoDB client is connected to MongoDB Atlas.

    Args:
        client: MongoDB client instance

    Returns:
        True if connected to Atlas, False otherwise
    """
    try:
        build_info = client.admin.command("buildInfo")
        modules = build_info.get("modules", [])
        is_atlas_deployment = "enterprise" in modules or "atlas" in str(
            build_info
        ).lower()
        logger.debug(f"Atlas detection: {is_atlas_deployment} (modules: {modules})")
        return is_atlas_deployment
    except Exception as e:
        logger.warning(f"Failed to detect Atlas deployment: {e}")
        return False


async def is_atlas_async(client: AsyncMongoClient) -> bool:
    """
    Async version: Detect if the MongoDB client is connected to MongoDB Atlas.

    Args:
        client: Async MongoDB client instance

    Returns:
        True if connected to Atlas, False otherwise
    """
    try:
        build_info = await client.admin.command("buildInfo")
        modules = build_info.get("modules", [])
        is_atlas_deployment = "enterprise" in modules or "atlas" in str(
            build_info
        ).lower()
        logger.debug(f"Atlas detection: {is_atlas_deployment} (modules: {modules})")
        return is_atlas_deployment
    except Exception as e:
        logger.warning(f"Failed to detect Atlas deployment: {e}")
        return False


def _parse_read_preference(read_preference: str) -> ReadPreference:
    """
    Convert string read preference to PyMongo ReadPreference enum.

    Args:
        read_preference: String representation of read preference

    Returns:
        ReadPreference enum value

    Raises:
        ValueError: If read_preference is not valid
    """
    preference_map = {
        "primary": ReadPreference.PRIMARY,
        "primaryPreferred": ReadPreference.PRIMARY_PREFERRED,
        "secondary": ReadPreference.SECONDARY,
        "secondaryPreferred": ReadPreference.SECONDARY_PREFERRED,
        "nearest": ReadPreference.NEAREST,
    }

    if read_preference not in preference_map:
        raise ValueError(
            f"Invalid read preference: {read_preference}. "
            f"Valid options: {list(preference_map.keys())}"
        )

    return preference_map[read_preference]


def get_collection_name(project: str, table_name: str) -> str:
    """
    Generate a MongoDB collection name from Feast project and table name.

    Args:
        project: Feast project name
        table_name: Feature view or table name

    Returns:
        Collection name in format: {project}_{table_name}
    """
    # Sanitize names to ensure valid MongoDB collection names
    # MongoDB collection names cannot contain: $ or null character
    # and should not start with "system."
    safe_project = project.replace("$", "_").replace("\x00", "")
    safe_table = table_name.replace("$", "_").replace("\x00", "")

    return f"{safe_project}_{safe_table}"

# TODO
#   - Consider removing this so that we don't create anything inside utils
#   - get_collection_name, though safe, could easily be done manually by the user's specification of project and table name. Unless $ is often in project names
#   - is_atlas methods. Are these accurate?
#   - all the kwargs passed to get_client_connection are not needed. Just put in `connection_string`
#   - _parse_read_preference is unneeded
