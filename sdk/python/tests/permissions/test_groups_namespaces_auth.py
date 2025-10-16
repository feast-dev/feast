"""
Tests for groups and namespaces authentication functionality.
"""

from feast.permissions.policy import (
    CombinedGroupNamespacePolicy,
    GroupBasedPolicy,
    NamespaceBasedPolicy,
    RoleBasedPolicy,
)
from feast.permissions.user import User


class TestUserGroupsNamespaces:
    """Test User class with groups and namespaces support."""

    def test_user_creation_with_groups_namespaces(self):
        """Test creating a user with groups and namespaces."""
        user = User(
            username="testuser",
            roles=["feast-reader"],
            groups=["data-team", "ml-engineers"],
            namespaces=["production", "staging"],
        )

        assert user.username == "testuser"
        assert user.roles == ["feast-reader"]
        assert user.groups == ["data-team", "ml-engineers"]
        assert user.namespaces == ["production", "staging"]

    def test_user_creation_without_groups_namespaces(self):
        """Test creating a user without groups and namespaces (backward compatibility)."""
        user = User(username="testuser", roles=["feast-reader"])

        assert user.username == "testuser"
        assert user.roles == ["feast-reader"]
        assert user.groups == []
        assert user.namespaces == []

    def test_has_matching_group(self):
        """Test group matching functionality."""
        user = User(
            username="testuser",
            roles=[],
            groups=["data-team", "ml-engineers"],
            namespaces=[],
        )

        assert user.has_matching_group(["data-team"])
        assert user.has_matching_group(["ml-engineers"])
        assert user.has_matching_group(["data-team", "other-team"])
        assert not user.has_matching_group(["other-team"])
        assert not user.has_matching_group([])

    def test_has_matching_namespace(self):
        """Test namespace matching functionality."""
        user = User(
            username="testuser",
            roles=[],
            groups=[],
            namespaces=["production", "staging"],
        )

        assert user.has_matching_namespace(["production"])
        assert user.has_matching_namespace(["staging"])
        assert user.has_matching_namespace(["production", "test"])
        assert not user.has_matching_namespace(["test"])
        assert not user.has_matching_namespace([])


class TestGroupBasedPolicy:
    """Test GroupBasedPolicy functionality."""

    def test_group_based_policy_validation(self):
        """Test group-based policy validation."""
        policy = GroupBasedPolicy(groups=["data-team", "ml-engineers"])

        # User with matching group
        user_with_group = User(
            username="testuser", roles=[], groups=["data-team"], namespaces=[]
        )
        result, explain = policy.validate_user(user_with_group)
        assert result is True
        assert explain == ""

        # User without matching group
        user_without_group = User(
            username="testuser", roles=[], groups=["other-team"], namespaces=[]
        )
        result, explain = policy.validate_user(user_without_group)
        assert result is False
        assert "permitted groups" in explain

    def test_group_based_policy_equality(self):
        """Test group-based policy equality."""
        policy1 = GroupBasedPolicy(groups=["data-team", "ml-engineers"])
        policy2 = GroupBasedPolicy(groups=["ml-engineers", "data-team"])
        policy3 = GroupBasedPolicy(groups=["other-team"])

        assert policy1 == policy2
        assert policy1 != policy3


class TestNamespaceBasedPolicy:
    """Test NamespaceBasedPolicy functionality."""

    def test_namespace_based_policy_validation(self):
        """Test namespace-based policy validation."""
        policy = NamespaceBasedPolicy(namespaces=["production", "staging"])

        # User with matching namespace
        user_with_namespace = User(
            username="testuser", roles=[], groups=[], namespaces=["production"]
        )
        result, explain = policy.validate_user(user_with_namespace)
        assert result is True
        assert explain == ""

        # User without matching namespace
        user_without_namespace = User(
            username="testuser", roles=[], groups=[], namespaces=["test"]
        )
        result, explain = policy.validate_user(user_without_namespace)
        assert result is False
        assert "permitted namespaces" in explain

    def test_namespace_based_policy_equality(self):
        """Test namespace-based policy equality."""
        policy1 = NamespaceBasedPolicy(namespaces=["production", "staging"])
        policy2 = NamespaceBasedPolicy(namespaces=["staging", "production"])
        policy3 = NamespaceBasedPolicy(namespaces=["test"])

        assert policy1 == policy2
        assert policy1 != policy3


class TestCombinedGroupNamespacePolicy:
    """Test CombinedGroupNamespacePolicy functionality."""

    def test_combined_policy_validation_both_match(self):
        """Test combined policy validation when both group and namespace match."""
        policy = CombinedGroupNamespacePolicy(
            groups=["data-team"], namespaces=["production"]
        )

        user = User(
            username="testuser",
            roles=[],
            groups=["data-team"],
            namespaces=["production"],
        )
        result, explain = policy.validate_user(user)
        assert result is True
        assert explain == ""

    def test_combined_policy_validation_group_matches_namespace_doesnt(self):
        """Test combined policy validation when group matches but namespace doesn't."""
        policy = CombinedGroupNamespacePolicy(
            groups=["data-team"], namespaces=["production"]
        )

        user = User(
            username="testuser", roles=[], groups=["data-team"], namespaces=["staging"]
        )
        result, _ = policy.validate_user(user)
        assert result is True

    def test_combined_policy_validation_namespace_matches_group_doesnt(self):
        """Test combined policy validation when namespace matches but group doesn't."""
        policy = CombinedGroupNamespacePolicy(
            groups=["data-team"], namespaces=["production"]
        )

        user = User(
            username="testuser",
            roles=[],
            groups=["other-team"],
            namespaces=["production"],
        )
        result, _ = policy.validate_user(user)
        assert result is True

    def test_combined_policy_validation_neither_matches(self):
        """Test combined policy validation when neither group nor namespace matches."""
        policy = CombinedGroupNamespacePolicy(
            groups=["data-team"], namespaces=["production"]
        )

        user = User(
            username="testuser", roles=[], groups=["other-team"], namespaces=["staging"]
        )
        result, _ = policy.validate_user(user)
        assert result is False

    def test_combined_policy_equality(self):
        """Test combined policy equality."""
        policy1 = CombinedGroupNamespacePolicy(
            groups=["data-team"], namespaces=["production"]
        )
        policy2 = CombinedGroupNamespacePolicy(
            groups=["data-team"], namespaces=["production"]
        )
        policy3 = CombinedGroupNamespacePolicy(
            groups=["other-team"], namespaces=["production"]
        )

        assert policy1 == policy2
        assert policy1 != policy3

    def test_combined_policy_proto_serialization(self):
        """Test CombinedGroupNamespacePolicy protobuf serialization and deserialization."""
        policy = CombinedGroupNamespacePolicy(
            groups=["data-team"], namespaces=["production"]
        )

        # Test to_proto
        proto = policy.to_proto()
        assert proto.HasField("combined_group_namespace_policy")
        assert list(proto.combined_group_namespace_policy.groups) == ["data-team"]
        assert list(proto.combined_group_namespace_policy.namespaces) == ["production"]

        # Test from_proto
        restored_policy = CombinedGroupNamespacePolicy.from_proto(proto)
        assert restored_policy.groups == ["data-team"]
        assert restored_policy.namespaces == ["production"]
        assert policy == restored_policy


class TestBackwardCompatibility:
    """Test backward compatibility with existing role-based policies."""

    def test_role_based_policy_still_works(self):
        """Test that existing role-based policies still work."""
        policy = RoleBasedPolicy(roles=["feast-reader"])

        user = User(
            username="testuser",
            roles=["feast-reader"],
            groups=["data-team"],
            namespaces=["production"],
        )
        result, explain = policy.validate_user(user)
        assert result is True

    def test_user_with_groups_namespaces_works_with_role_policy(self):
        """Test that users with groups and namespaces work with role-based policies."""
        policy = RoleBasedPolicy(roles=["feast-reader"])

        user = User(
            username="testuser",
            roles=["feast-reader"],
            groups=["data-team"],
            namespaces=["production"],
        )
        result, _ = policy.validate_user(user)
        assert result is True
