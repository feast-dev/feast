import logging
import os

import jwt
from kubernetes import client, config
from starlette.authentication import (
    AuthenticationError,
)

from feast.permissions.auth.token_parser import TokenParser
from feast.permissions.user import User

logger = logging.getLogger(__name__)
_namespace_file_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"


class KubernetesTokenParser(TokenParser):
    """
    A `TokenParser` implementation to use Kubernetes RBAC resources to retrieve the user details.
    The assumption is that the request header includes an authorization bearer with the token of the
    client `ServiceAccount`.
    By inspecting the role bindings, this `TokenParser` extracts the associated `Role`s.

    The client `ServiceAccount` is instead used as the user name, together with the current namespace.
    """

    def __init__(self):
        config.load_incluster_config()
        self.v1 = client.CoreV1Api()
        self.rbac_v1 = client.RbacAuthorizationV1Api()
        self.auth_v1 = client.AuthenticationV1Api()

    async def user_details_from_access_token(self, access_token: str) -> User:
        """
        Extract user details from the token using Token Access Review.
        Handles both service account tokens (JWTs) and user tokens (opaque tokens).

        Returns:
            User: Current user, with associated roles, groups, and namespaces.

        Raises:
            AuthenticationError if any error happens.
        """
        # First, try to extract user information using Token Access Review
        groups, namespaces = self._extract_groups_and_namespaces_from_token(
            access_token
        )

        # Try to determine if this is a service account or regular user
        try:
            # Attempt to decode as JWT (for service accounts)
            sa_namespace, sa_name = _decode_token(access_token)
            current_user = f"{sa_namespace}:{sa_name}"
            logger.info(
                f"Request received from ServiceAccount: {sa_name} in namespace: {sa_namespace}"
            )

            intra_communication_base64 = os.getenv("INTRA_COMMUNICATION_BASE64")
            if sa_name is not None and sa_name == intra_communication_base64:
                return User(username=sa_name, roles=[], groups=[], namespaces=[])
            else:
                current_namespace = self._read_namespace_from_file()
                logger.info(
                    f"Looking for ServiceAccount roles of {sa_namespace}:{sa_name} in {current_namespace}"
                )

                # Get roles using existing method
                roles = self.get_roles(
                    current_namespace=current_namespace,
                    service_account_namespace=sa_namespace,
                    service_account_name=sa_name,
                )
                logger.info(f"Roles: {roles}")

                return User(
                    username=current_user,
                    roles=roles,
                    groups=groups,
                    namespaces=namespaces,
                )

        except AuthenticationError as e:
            # If JWT decoding fails, this is likely a user token
            # Use Token Access Review to get user information
            logger.info(f"Token is not a JWT (likely a user token): {e}")

            # Get username from Token Access Review
            username = self._get_username_from_token_review(access_token)
            if not username:
                raise AuthenticationError("Could not extract username from token")

            logger.info(f"Request received from User: {username}")

            # Extract roles for the user from RoleBindings and ClusterRoleBindings
            logger.info(f"Extracting roles for user {username} with groups: {groups}")
            roles = self.get_user_roles(username, groups)

            return User(
                username=username, roles=roles, groups=groups, namespaces=namespaces
            )

    def _read_namespace_from_file(self):
        try:
            with open(_namespace_file_path, "r") as file:
                namespace = file.read().strip()
            return namespace
        except Exception as e:
            raise e

    def get_roles(
        self,
        current_namespace: str,
        service_account_namespace: str,
        service_account_name: str,
    ) -> list[str]:
        """
        Fetches the Kubernetes `Role`s associated to the given `ServiceAccount` in `current_namespace` namespace.

        The running deployment must be granted enough permissions to query for such instances in this namespace.

        Returns:
            list[str]: Name of the `Role`s associated to the service account. No string manipulation is performed on the role name.
        """
        role_bindings = self.rbac_v1.list_namespaced_role_binding(current_namespace)
        roles: set[str] = set()

        for binding in role_bindings.items:
            if binding.subjects is not None:
                for subject in binding.subjects:
                    if (
                        subject.kind == "ServiceAccount"
                        and subject.name == service_account_name
                        and subject.namespace == service_account_namespace
                    ):
                        roles.add(binding.role_ref.name)

        return list(roles)

    def get_user_roles(self, username: str, groups: list[str]) -> list[str]:
        """
        Fetches the Kubernetes `Role`s and `ClusterRole`s associated to the given user and their groups.

        This method checks both namespaced RoleBindings and ClusterRoleBindings for:
        - Direct user assignments
        - Group-based assignments where the user is a member

        Returns:
            list[str]: Names of the `Role`s and `ClusterRole`s associated to the user.
        """
        roles: set[str] = set()

        try:
            # Get all namespaced RoleBindings across all namespaces
            all_role_bindings = self.rbac_v1.list_role_binding_for_all_namespaces()

            for binding in all_role_bindings.items:
                if binding.subjects is not None:
                    for subject in binding.subjects:
                        # Check for direct user assignment
                        if subject.kind == "User" and subject.name == username:
                            roles.add(binding.role_ref.name)
                        # Check for group-based assignment
                        elif subject.kind == "Group" and subject.name in groups:
                            roles.add(binding.role_ref.name)

            # Get all ClusterRoleBindings
            cluster_role_bindings = self.rbac_v1.list_cluster_role_binding()

            for binding in cluster_role_bindings.items:
                if binding.subjects is not None:
                    for subject in binding.subjects:
                        # Check for direct user assignment
                        if subject.kind == "User" and subject.name == username:
                            roles.add(binding.role_ref.name)
                        # Check for group-based assignment
                        elif subject.kind == "Group" and subject.name in groups:
                            roles.add(binding.role_ref.name)

            logger.info(f"Found {len(roles)} roles for user {username}: {list(roles)}")

        except Exception as e:
            logger.error(f"Failed to extract user roles for {username}: {e}")

        return list(roles)

    def _extract_groups_and_namespaces_from_token(
        self, access_token: str
    ) -> tuple[list[str], list[str]]:
        """
        Extract groups and namespaces from the token using Kubernetes Token Access Review.

        Args:
            access_token: The JWT token to analyze

        Returns:
            tuple[list[str], list[str]]: A tuple containing (groups, namespaces)
        """
        try:
            # Create TokenReview object
            token_review = client.V1TokenReview(
                spec=client.V1TokenReviewSpec(token=access_token)
            )
            groups: list[str] = []
            namespaces: list[str] = []

            # Call Token Access Review API
            response = self.auth_v1.create_token_review(token_review)

            if response.status.authenticated:
                # Extract groups and namespaces from the response
                # Groups are in response.status.user.groups, not response.status.groups
                if response.status.user and hasattr(response.status.user, "groups"):
                    groups = response.status.user.groups or []
                else:
                    groups = []

                # Extract namespaces from the user info
                if response.status.user:
                    username = getattr(response.status.user, "username", "") or ""

                    if ":" in username and username.startswith(
                        "system:serviceaccount:"
                    ):
                        # Service account logic - extract namespace from username
                        parts = username.split(":")
                        if len(parts) >= 4:
                            service_account_namespace = parts[
                                2
                            ]  # namespace is the 3rd part
                            namespaces.append(service_account_namespace)

                            # For service accounts, also extract groups that have access to this namespace
                            namespace_groups = self._extract_namespace_access_groups(
                                service_account_namespace
                            )
                            groups.extend(namespace_groups)
                    else:
                        # Regular user logic - extract namespaces from dashboard-permissions RoleBindings
                        user_namespaces = self._extract_user_project_namespaces(
                            username
                        )
                        namespaces.extend(user_namespaces)
                        logger.info(
                            f"Found {len(user_namespaces)} data science projects for user {username}: {user_namespaces}"
                        )

                    # Also check if there are namespace-specific groups
                    for group in groups:
                        if group.startswith("system:serviceaccounts:"):
                            # Extract namespace from service account group
                            parts = group.split(":")
                            if len(parts) >= 3:
                                namespaces.append(parts[2])

                logger.debug(
                    f"Token Access Review successful. Groups: {groups}, Namespaces: {namespaces}"
                )
            else:
                logger.warning(f"Token Access Review failed: {response.status.error}")

        except Exception as e:
            logger.error(f"Failed to perform Token Access Review: {e}")
            # We dont need to extract groups and namespaces from jwt decoding, not ideal for kubernetes auth

        # Remove duplicates
        groups = sorted(list(set(groups)))
        namespaces = sorted(list(set(namespaces)))
        return groups, namespaces

    def _extract_namespace_access_groups(self, namespace: str) -> list[str]:
        """
        Extract groups that have access to a specific namespace by querying RoleBindings and ClusterRoleBindings.

        Args:
            namespace: The namespace to check for group access

        Returns:
            list[str]: List of groups that have access to the namespace
        """
        groups = []
        try:
            # Get RoleBindings in the namespace
            role_bindings = self.rbac_v1.list_namespaced_role_binding(
                namespace=namespace
            )
            for rb in role_bindings.items:
                for subject in rb.subjects or []:
                    if subject.kind == "Group":
                        groups.append(subject.name)
                        logger.debug(
                            f"Found group {subject.name} in RoleBinding {rb.metadata.name}"
                        )

            # Get ClusterRoleBindings that might grant access to this namespace
            cluster_role_bindings = self.rbac_v1.list_cluster_role_binding()
            for crb in cluster_role_bindings.items:
                # Check if this ClusterRoleBinding grants access to the namespace
                if self._cluster_role_binding_grants_namespace_access(crb, namespace):
                    for subject in crb.subjects or []:
                        if subject.kind == "Group":
                            groups.append(subject.name)
                            logger.debug(
                                f"Found group {subject.name} in ClusterRoleBinding {crb.metadata.name}"
                            )

            # Remove duplicates and sort
            groups = sorted(list(set(groups)))
            logger.info(
                f"Found {len(groups)} groups with access to namespace {namespace}: {groups}"
            )

        except Exception as e:
            logger.error(
                f"Failed to extract namespace access groups for {namespace}: {e}"
            )

        return groups

    def _extract_user_project_namespaces(self, username: str) -> list[str]:
        """
        Extract data science project namespaces where a user has been added via dashboard-permissions RoleBindings.

        This method queries all RoleBindings where the user is a subject and filters for
        'dashboard-permissions-*' RoleBindings or 'admin' RoleBindings, which indicate the user has been added to that data science project.

        Args:
            username: The username to search for in RoleBindings

        Returns:
            list[str]: List of namespace names where the user has dashboard or admin permissions
        """
        user_namespaces = []
        try:
            # Query all RoleBindings where the user is a subject
            # This is much more efficient than scanning all namespaces
            all_role_bindings = self.rbac_v1.list_role_binding_for_all_namespaces()

            for rb in all_role_bindings.items:
                # Check if this is a dashboard-permissions RoleBinding
                is_dashboard_permissions = (
                    rb.metadata.name.startswith("dashboard-permissions-")
                    and rb.metadata.labels
                    and rb.metadata.labels.get("opendatahub.io/dashboard") == "true"
                )

                # Check if this is an admin RoleBinding
                is_admin_rolebinding = (
                    rb.role_ref
                    and rb.role_ref.kind == "ClusterRole"
                    and rb.role_ref.name == "admin"
                )

                if is_dashboard_permissions or is_admin_rolebinding:
                    # Check if the user is a subject in this RoleBinding
                    for subject in rb.subjects or []:
                        if subject.kind == "User" and subject.name == username:
                            namespace_name = rb.metadata.namespace
                            user_namespaces.append(namespace_name)
                            rolebinding_type = (
                                "dashboard-permissions"
                                if is_dashboard_permissions
                                else "admin"
                            )
                            logger.debug(
                                f"Found user {username} in {rolebinding_type} RoleBinding "
                                f"{rb.metadata.name} in namespace {namespace_name}"
                            )
                            break  # Found the user in this RoleBinding, no need to check other subjects

            # Remove duplicates and sort
            user_namespaces = sorted(list(set(user_namespaces)))
            logger.info(
                f"User {username} has dashboard or admin permissions in {len(user_namespaces)} namespaces: {user_namespaces}"
            )

        except Exception as e:
            logger.error(
                f"Failed to extract user data science projects for {username}: {e}"
            )

        return user_namespaces

    def _get_username_from_token_review(self, access_token: str) -> str:
        """
        Extract username from Token Access Review.

        Args:
            access_token: The access token to review

        Returns:
            str: The username from the token review, or empty string if not found
        """
        try:
            token_review = client.V1TokenReview(
                spec=client.V1TokenReviewSpec(token=access_token)
            )

            response = self.auth_v1.create_token_review(token_review)

            if response.status.authenticated and response.status.user:
                username = getattr(response.status.user, "username", "") or ""
                logger.debug(f"Extracted username from Token Access Review: {username}")
                return username
            else:
                logger.warning(f"Token Access Review failed: {response.status.error}")
                return ""

        except Exception as e:
            logger.error(f"Failed to get username from Token Access Review: {e}")
            return ""

    def _cluster_role_binding_grants_namespace_access(
        self, cluster_role_binding, namespace: str
    ) -> bool:
        """
        Check if a ClusterRoleBinding grants access to a specific namespace.
        This is a simplified check - in practice, you might need more sophisticated logic.

        Args:
            cluster_role_binding: The ClusterRoleBinding to check
            namespace: The namespace to check access for

        Returns:
            bool: True if the ClusterRoleBinding likely grants access to the namespace
        """
        try:
            # Get the ClusterRole referenced by this binding
            cluster_role_name = cluster_role_binding.role_ref.name
            cluster_role = self.rbac_v1.read_cluster_role(name=cluster_role_name)

            # Check if the ClusterRole has rules that could grant access to the namespace
            for rule in cluster_role.rules or []:
                # Check if the rule applies to namespaces or has wildcard access
                if (
                    rule.resources
                    and ("namespaces" in rule.resources or "*" in rule.resources)
                    and rule.verbs
                    and (
                        "get" in rule.verbs or "list" in rule.verbs or "*" in rule.verbs
                    )
                ):
                    return True

                # Check if the rule has resourceNames that include our namespace
                if rule.resource_names and namespace in rule.resource_names:
                    return True

        except Exception as e:
            logger.debug(
                f"Error checking ClusterRoleBinding {cluster_role_binding.metadata.name}: {e}"
            )

        return False


def _decode_token(access_token: str) -> tuple[str, str]:
    """
    The `sub` portion of the decoded token includes the service account name in the format: `system:serviceaccount:NAMESPACE:SA_NAME`

    Returns:
        str: the namespace name.
        str: the `ServiceAccount` name.
    """
    try:
        decoded_token = jwt.decode(access_token, options={"verify_signature": False})
        if "sub" in decoded_token:
            subject: str = decoded_token["sub"]
            if len(subject.split(":")) != 4:
                raise AuthenticationError(
                    f"Expecting 4 elements separated by : in th subject section, instead of {len(subject.split(':'))}."
                )
            _, _, sa_namespace, sa_name = subject.split(":")
            return (sa_namespace, sa_name)
        else:
            raise AuthenticationError("Missing sub section in received token.")
    except jwt.DecodeError as e:
        raise AuthenticationError(f"Error decoding JWT token: {e}")
