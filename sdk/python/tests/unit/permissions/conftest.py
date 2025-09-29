from unittest.mock import MagicMock, Mock

import pytest

from feast import FeatureView
from feast.data_source import DataSource
from feast.entity import Entity
from feast.infra.registry.base_registry import BaseRegistry
from feast.permissions.decorator import require_permissions
from feast.permissions.permission import AuthzedAction, Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.permissions.security_manager import (
    SecurityManager,
    set_security_manager,
)
from feast.permissions.user import User


class SecuredFeatureView(FeatureView):
    def __init__(self, name, tags):
        mock_source = MagicMock(spec=DataSource)
        mock_source.created_timestamp_column = None
        mock_source.timestamp_field = None
        mock_source.date_partition_column = None

        super().__init__(
            name=name,
            source=mock_source,
            tags=tags,
        )

    @require_permissions(actions=[AuthzedAction.DESCRIBE])
    def read_protected(self) -> bool:
        return True

    @require_permissions(actions=[AuthzedAction.UPDATE])
    def write_protected(self) -> bool:
        return True

    def unprotected(self) -> bool:
        return True


@pytest.fixture
def feature_views() -> list[FeatureView]:
    return [
        SecuredFeatureView("secured", {}),
        SecuredFeatureView("special-secured", {}),
    ]


@pytest.fixture
def users() -> list[User]:
    users = []
    users.append(User("r", ["reader"]))
    users.append(User("w", ["writer"]))
    users.append(User("rw", ["reader", "writer"]))
    users.append(User("special", ["reader", "writer", "special-reader"]))
    users.append(User("updater", ["updater"]))
    users.append(User("creator", ["creator"]))
    users.append(User("admin", ["updater", "creator"]))
    return dict([(u.username, u) for u in users])


@pytest.fixture
def security_manager() -> SecurityManager:
    permissions = []
    permissions.append(
        Permission(
            name="reader",
            types=FeatureView,
            policy=RoleBasedPolicy(roles=["reader"]),
            actions=[AuthzedAction.DESCRIBE],
        )
    )
    permissions.append(
        Permission(
            name="writer",
            types=FeatureView,
            policy=RoleBasedPolicy(roles=["writer"]),
            actions=[AuthzedAction.UPDATE],
        )
    )
    permissions.append(
        Permission(
            name="special",
            types=FeatureView,
            name_patterns="special.*",
            policy=RoleBasedPolicy(roles=["special-reader"]),
            actions=[AuthzedAction.DESCRIBE, AuthzedAction.UPDATE],
        )
    )
    permissions.append(
        Permission(
            name="entity_updater",
            types=Entity,
            policy=RoleBasedPolicy(roles=["updater"]),
            actions=[AuthzedAction.DESCRIBE, AuthzedAction.UPDATE],
        )
    )
    permissions.append(
        Permission(
            name="entity_creator",
            types=Entity,
            policy=RoleBasedPolicy(roles=["creator"]),
            actions=[AuthzedAction.CREATE],
        )
    )

    registry = Mock(spec=BaseRegistry)
    registry.list_permissions = Mock(return_value=permissions)
    sm = SecurityManager(project="any", registry=registry)
    set_security_manager(sm)
    return sm
