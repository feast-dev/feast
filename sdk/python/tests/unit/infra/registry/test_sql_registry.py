# Copyright 2021 The Feast Authors
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

import multiprocessing
import os
import sys
import tempfile

import pytest

from feast.entity import Entity
from feast.infra.registry.sql import SqlRegistry, SqlRegistryConfig


@pytest.fixture
def sqlite_registry():
    """Create a temporary SQLite registry for testing."""
    fd, registry_path = tempfile.mkstemp()
    registry_config = SqlRegistryConfig(
        registry_type="sql",
        path=f"sqlite:///{registry_path}",
        purge_feast_metadata=False,
    )

    registry = SqlRegistry(registry_config, "test_project", None)
    yield registry
    registry.teardown()


def test_sql_registry(sqlite_registry):
    """
    Test the SQL registry
    """
    entity = Entity(
        name="test_entity",
        description="Test entity for testing",
        tags={"test": "transaction"},
    )
    sqlite_registry.apply_entity(entity, "test_project")
    retrieved_entity = sqlite_registry.get_entity("test_entity", "test_project")
    assert retrieved_entity.name == "test_entity"
    assert retrieved_entity.description == "Test entity for testing"

    sqlite_registry.set_project_metadata("test_project", "test_key", "test_value")
    value = sqlite_registry.get_project_metadata("test_project", "test_key")
    assert value == "test_value"

    sqlite_registry.delete_entity("test_entity", "test_project")
    with pytest.raises(Exception):
        sqlite_registry.get_entity("test_entity", "test_project")


def test_sql_registry_reinitialize_engines():
    """
    Test that reinitialize_engines() properly disposes and recreates engines.

    This is critical for fork-safety when using multi-worker servers.
    """
    fd, registry_path = tempfile.mkstemp()
    registry_config = SqlRegistryConfig(
        registry_type="sql",
        path=f"sqlite:///{registry_path}",
        purge_feast_metadata=False,
    )

    registry = SqlRegistry(registry_config, "test_project", None)

    # Store original engine references
    original_write_engine = registry.write_engine
    original_read_engine = registry.read_engine

    # Apply an entity before reinitializing
    entity = Entity(
        name="test_entity",
        description="Test entity before reinitialize",
    )
    registry.apply_entity(entity, "test_project")

    # Reinitialize engines
    registry.reinitialize_engines()

    # Verify engines are new instances
    assert registry.write_engine is not original_write_engine
    assert registry.read_engine is not original_read_engine

    # Verify the registry still works after reinitialization
    retrieved_entity = registry.get_entity("test_entity", "test_project")
    assert retrieved_entity.name == "test_entity"

    # Apply a new entity after reinitializing
    entity2 = Entity(
        name="test_entity_2",
        description="Test entity after reinitialize",
    )
    registry.apply_entity(entity2, "test_project")
    retrieved_entity2 = registry.get_entity("test_entity_2", "test_project")
    assert retrieved_entity2.name == "test_entity_2"

    registry.teardown()


def test_sql_registry_on_worker_init():
    """
    Test that on_worker_init() properly reinitializes the registry.

    This method should be called after a process fork to ensure
    the registry has fresh database connections.
    """
    fd, registry_path = tempfile.mkstemp()
    registry_config = SqlRegistryConfig(
        registry_type="sql",
        path=f"sqlite:///{registry_path}",
        purge_feast_metadata=False,
    )

    registry = SqlRegistry(registry_config, "test_project", None)

    # Store original engine reference
    original_write_engine = registry.write_engine

    # Apply an entity before on_worker_init
    entity = Entity(
        name="test_entity",
        description="Test entity before worker init",
    )
    registry.apply_entity(entity, "test_project")

    # Call on_worker_init (simulates what happens after fork)
    registry.on_worker_init()

    # Verify engine was recreated
    assert registry.write_engine is not original_write_engine

    # Verify the registry still works
    retrieved_entity = registry.get_entity("test_entity", "test_project")
    assert retrieved_entity.name == "test_entity"

    registry.teardown()


def test_sql_registry_with_separate_read_write_engines():
    """
    Test reinitialize_engines with separate read and write paths.
    """
    fd1, write_path = tempfile.mkstemp()
    fd2, read_path = tempfile.mkstemp()

    # Use the same path for both (SQLite doesn't support true read replicas,
    # but this tests the code path)
    registry_config = SqlRegistryConfig(
        registry_type="sql",
        path=f"sqlite:///{write_path}",
        read_path=f"sqlite:///{write_path}",  # Same path to share data
        purge_feast_metadata=False,
    )

    registry = SqlRegistry(registry_config, "test_project", None)

    # When read_path is specified, read_engine should be different from write_engine
    assert registry.read_engine is not registry.write_engine

    original_write_engine = registry.write_engine
    original_read_engine = registry.read_engine

    # Apply entity
    entity = Entity(name="test_entity", description="Test")
    registry.apply_entity(entity, "test_project")

    # Reinitialize
    registry.reinitialize_engines()

    # Both engines should be new
    assert registry.write_engine is not original_write_engine
    assert registry.read_engine is not original_read_engine
    assert registry.read_engine is not registry.write_engine

    # Verify still works
    retrieved = registry.get_entity("test_entity", "test_project")
    assert retrieved.name == "test_entity"

    registry.teardown()


@pytest.mark.skipif(
    sys.platform == "win32", reason="Fork not available on Windows"
)
def test_sql_registry_fork_safety():
    """
    Test that SqlRegistry works correctly after a process fork.

    This test simulates what happens when Gunicorn forks worker processes.
    Each worker should be able to use the registry after calling on_worker_init().
    """
    fd, registry_path = tempfile.mkstemp()
    registry_config = SqlRegistryConfig(
        registry_type="sql",
        path=f"sqlite:///{registry_path}",
        purge_feast_metadata=False,
    )

    registry = SqlRegistry(registry_config, "test_project", None)

    # Apply an entity in the parent process
    entity = Entity(
        name="parent_entity",
        description="Created in parent process",
    )
    registry.apply_entity(entity, "test_project")

    def child_process_work(result_queue):
        """Work done in child process after fork."""
        try:
            # This simulates what the post_fork hook does
            registry.on_worker_init()

            # Try to read the entity created by parent
            retrieved = registry.get_entity("parent_entity", "test_project")
            assert retrieved.name == "parent_entity"

            # Try to create a new entity in the child
            child_entity = Entity(
                name=f"child_entity_{os.getpid()}",
                description="Created in child process",
            )
            registry.apply_entity(child_entity, "test_project")

            # Verify we can read it back
            retrieved_child = registry.get_entity(
                f"child_entity_{os.getpid()}", "test_project"
            )
            assert retrieved_child.name == f"child_entity_{os.getpid()}"

            result_queue.put(("success", None))
        except Exception as e:
            result_queue.put(("error", str(e)))

    # Use multiprocessing to simulate fork
    result_queue = multiprocessing.Queue()
    child = multiprocessing.Process(target=child_process_work, args=(result_queue,))
    child.start()
    child.join(timeout=30)

    # Check result
    assert not result_queue.empty(), "Child process did not return a result"
    status, error = result_queue.get()
    assert status == "success", f"Child process failed: {error}"

    registry.teardown()
