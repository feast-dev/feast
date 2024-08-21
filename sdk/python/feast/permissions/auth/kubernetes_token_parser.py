import logging

import jwt
from kubernetes import client, config
from starlette.authentication import (
    AuthenticationError,
)

from feast.permissions.auth.token_parser import TokenParser
from feast.permissions.user import User

logger = logging.getLogger(__name__)


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

    async def user_details_from_access_token(self, access_token: str) -> User:
        """
        Extract the service account from the token and search the roles associated with it.

        Returns:
            User: Current user, with associated roles. The `username` is the `:` separated concatenation of `namespace` and `service account name`.

        Raises:
            AuthenticationError if any error happens.
        """
        sa_namespace, sa_name = _decode_token(access_token)
        current_user = f"{sa_namespace}:{sa_name}"
        logging.info(f"Received request from {sa_name} in {sa_namespace}")

        roles = self.get_roles(sa_namespace, sa_name)
        logging.info(f"SA roles are: {roles}")

        return User(username=current_user, roles=roles)

    def get_roles(self, namespace: str, service_account_name: str) -> list[str]:
        """
        Fetches the Kubernetes `Role`s associated to the given `ServiceAccount` in the given `namespace`.

        The research also includes the `ClusterRole`s, so the running deployment must be granted enough permissions to query
        for such instances in all the namespaces.

        Returns:
            list[str]: Name of the `Role`s and `ClusterRole`s associated to the service account. No string manipulation is performed on the role name.
        """
        role_bindings = self.rbac_v1.list_namespaced_role_binding(namespace)
        cluster_role_bindings = self.rbac_v1.list_cluster_role_binding()

        roles: set[str] = set()

        for binding in role_bindings.items:
            if binding.subjects is not None:
                for subject in binding.subjects:
                    if (
                        subject.kind == "ServiceAccount"
                        and subject.name == service_account_name
                    ):
                        roles.add(binding.role_ref.name)

        for binding in cluster_role_bindings.items:
            if binding.subjects is not None:
                for subject in binding.subjects:
                    if (
                        subject.kind == "ServiceAccount"
                        and subject.name == service_account_name
                        and subject.namespace == namespace
                    ):
                        roles.add(binding.role_ref.name)

        return list(roles)


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
