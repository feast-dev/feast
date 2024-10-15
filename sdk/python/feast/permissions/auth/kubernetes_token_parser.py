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
        logger.info(
            f"Request received from ServiceAccount: {sa_name} in namespace: {sa_namespace}"
        )

        intra_communication_base64 = os.getenv("INTRA_COMMUNICATION_BASE64")
        if sa_name is not None and sa_name == intra_communication_base64:
            return User(username=sa_name, roles=[])
        else:
            current_namespace = self._read_namespace_from_file()
            logger.info(
                f"Looking for ServiceAccount roles of {sa_namespace}:{sa_name} in {current_namespace}"
            )
            roles = self.get_roles(
                current_namespace=current_namespace,
                service_account_namespace=sa_namespace,
                service_account_name=sa_name,
            )
            logger.info(f"Roles: {roles}")

            return User(username=current_user, roles=roles)

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
