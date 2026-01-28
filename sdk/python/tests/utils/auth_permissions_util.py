import os
import subprocess
from pathlib import Path

import yaml
from keycloak import KeycloakAdmin

from feast import (
    FeatureStore,
    RepoConfig,
)
from feast.infra.registry.remote import RemoteRegistryConfig
from feast.permissions.permission import Permission
from feast.wait import wait_retry_backoff
from tests.utils.cli_repo_creator import CliRunner
from tests.utils.http_server import check_port_open

PROJECT_NAME = "feast_test_project"


def include_auth_config(file_path, auth_config: str):
    with open(file_path, "r") as file:
        existing_content = yaml.safe_load(file)
    new_section = yaml.safe_load(auth_config)
    if isinstance(existing_content, dict) and isinstance(new_section, dict):
        existing_content.update(new_section)
    else:
        raise ValueError("Both existing content and new section must be dictionaries.")
    with open(file_path, "w") as file:
        yaml.safe_dump(existing_content, file, default_flow_style=False)
    print(f"Updated auth section at {file_path}")


def default_store(
    temp_dir,
    auth_config: str,
    permissions: list[Permission],
):
    runner = CliRunner()
    result = runner.run(["init", PROJECT_NAME], cwd=Path(temp_dir))
    repo_path = os.path.join(temp_dir, PROJECT_NAME, "feature_repo")
    assert result.returncode == 0, (
        f"feast init failed. stdout:\n{result.stdout.decode(errors='ignore')}\n"
        f"stderr:\n{result.stderr.decode(errors='ignore')}\n"
    )

    include_auth_config(
        file_path=f"{repo_path}/feature_store.yaml", auth_config=auth_config
    )

    result = runner.run(["--chdir", repo_path, "apply"], cwd=Path(temp_dir))
    assert result.returncode == 0, (
        f"feast apply failed. stdout:\n{result.stdout.decode(errors='ignore')}\n"
        f"stderr:\n{result.stderr.decode(errors='ignore')}\n"
    )

    fs = FeatureStore(repo_path=repo_path)

    fs.apply(permissions)  # type: ignore

    return fs


def start_feature_server(
    repo_path: str,
    server_port: int,
    metrics: bool = False,
    tls_key_path: str = "",
    tls_cert_path: str = "",
    ca_trust_store_path: str = "",
):
    host = "0.0.0.0"
    cmd = [
        "feast",
        "-c" + repo_path,
        "serve",
        "--host",
        host,
        "--port",
        str(server_port),
    ]

    if tls_cert_path and tls_cert_path:
        cmd.append("--key")
        cmd.append(tls_key_path)
        cmd.append("--cert")
        cmd.append(tls_cert_path)

    feast_server_process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    _time_out_sec: int = 60
    # Wait for server to start
    wait_retry_backoff(
        lambda: (None, check_port_open(host, server_port)),
        timeout_secs=_time_out_sec,
        timeout_msg=f"Unable to start the feast server in {_time_out_sec} seconds for remote online store type, port={server_port}",
    )

    if metrics:
        cmd.append("--metrics")

    # Check if metrics are enabled and Prometheus server is running
    if metrics:
        wait_retry_backoff(
            lambda: (None, check_port_open("localhost", 8000)),
            timeout_secs=_time_out_sec,
            timeout_msg="Unable to start the Prometheus server in 60 seconds.",
        )
    else:
        assert not check_port_open("localhost", 8000), (
            "Prometheus server is running when it should be disabled."
        )

    online_server_url = (
        f"https://localhost:{server_port}"
        if tls_key_path and tls_cert_path
        else f"http://localhost:{server_port}"
    )

    yield (online_server_url)

    if feast_server_process is not None:
        feast_server_process.kill()

        # wait server to free the port
        wait_retry_backoff(
            lambda: (
                None,
                not check_port_open("localhost", server_port),
            ),
            timeout_msg=f"Unable to stop the feast server in {_time_out_sec} seconds for remote online store type, port={server_port}",
            timeout_secs=_time_out_sec,
        )


def get_remote_registry_store(server_port, feature_store, tls_mode):
    is_tls_mode, _, tls_cert_path, ca_trust_store_path = tls_mode
    if is_tls_mode:
        if ca_trust_store_path:
            registry_config = RemoteRegistryConfig(
                registry_type="remote",
                path=f"localhost:{server_port}",
                is_tls=True,
            )
        else:
            registry_config = RemoteRegistryConfig(
                registry_type="remote",
                path=f"localhost:{server_port}",
                is_tls=True,
                cert=tls_cert_path,
            )
    else:
        registry_config = RemoteRegistryConfig(
            registry_type="remote", path=f"localhost:{server_port}"
        )

    if is_tls_mode and ca_trust_store_path:
        # configure trust store path only when is_tls_mode and ca_trust_store_path exists.
        os.environ["FEAST_CA_CERT_FILE_PATH"] = ca_trust_store_path

    store = FeatureStore(
        config=RepoConfig(
            project=PROJECT_NAME,
            auth=feature_store.config.auth,
            registry=registry_config,
            provider="local",
            entity_key_serialization_version=3,
            repo_path=feature_store.repo_path,
        )
    )
    return store


def setup_permissions_on_keycloak(keycloak_admin: KeycloakAdmin):
    new_client_id = "feast-integration-client"
    new_client_secret = "feast-integration-client-secret"
    # Create a new client
    client_representation = {
        "clientId": new_client_id,
        "secret": new_client_secret,
        "enabled": True,
        "directAccessGrantsEnabled": True,
        "publicClient": False,
        "redirectUris": ["*"],
        "serviceAccountsEnabled": True,
        "standardFlowEnabled": True,
    }
    keycloak_admin.create_client(client_representation)

    # Get the client ID
    client_id = keycloak_admin.get_client_id(new_client_id)

    # Role representation
    reader_role_rep = {
        "name": "reader",
        "description": "feast reader client role",
        "composite": False,
        "clientRole": True,
        "containerId": client_id,
    }
    keycloak_admin.create_client_role(client_id, reader_role_rep, True)
    reader_role_id = keycloak_admin.get_client_role(
        client_id=client_id, role_name="reader"
    )

    # Role representation
    writer_role_rep = {
        "name": "writer",
        "description": "feast writer client role",
        "composite": False,
        "clientRole": True,
        "containerId": client_id,
    }
    keycloak_admin.create_client_role(client_id, writer_role_rep, True)
    writer_role_id = keycloak_admin.get_client_role(
        client_id=client_id, role_name="writer"
    )

    # Mapper representation
    mapper_representation = {
        "name": "client-roles-mapper",
        "protocol": "openid-connect",
        "protocolMapper": "oidc-usermodel-client-role-mapper",
        "consentRequired": False,
        "config": {
            "multivalued": "true",
            "userinfo.token.claim": "true",
            "id.token.claim": "true",
            "access.token.claim": "true",
            "claim.name": "roles",
            "jsonType.label": "String",
            "client.id": client_id,
        },
    }

    # Add predefined client roles mapper to the client
    keycloak_admin.add_mapper_to_client(client_id, mapper_representation)

    reader_writer_user = {
        "username": "reader_writer",
        "enabled": True,
        "firstName": "reader_writer fn",
        "lastName": "reader_writer ln",
        "email": "reader_writer@email.com",
        "emailVerified": True,
        "credentials": [{"value": "password", "type": "password", "temporary": False}],
    }
    reader_writer_user_id = keycloak_admin.create_user(reader_writer_user)
    keycloak_admin.assign_client_role(
        user_id=reader_writer_user_id,
        client_id=client_id,
        roles=[reader_role_id, writer_role_id],
    )

    reader_user = {
        "username": "reader",
        "enabled": True,
        "firstName": "reader fn",
        "lastName": "reader ln",
        "email": "reader@email.com",
        "emailVerified": True,
        "credentials": [{"value": "password", "type": "password", "temporary": False}],
    }
    reader_user_id = keycloak_admin.create_user(reader_user)
    keycloak_admin.assign_client_role(
        user_id=reader_user_id, client_id=client_id, roles=[reader_role_id]
    )

    writer_user = {
        "username": "writer",
        "enabled": True,
        "firstName": "writer fn",
        "lastName": "writer ln",
        "email": "writer@email.com",
        "emailVerified": True,
        "credentials": [{"value": "password", "type": "password", "temporary": False}],
    }
    writer_user_id = keycloak_admin.create_user(writer_user)
    keycloak_admin.assign_client_role(
        user_id=writer_user_id, client_id=client_id, roles=[writer_role_id]
    )

    no_roles_user = {
        "username": "no_roles_user",
        "enabled": True,
        "firstName": "no_roles_user fn",
        "lastName": "no_roles_user ln",
        "email": "no_roles_user@email.com",
        "emailVerified": True,
        "credentials": [{"value": "password", "type": "password", "temporary": False}],
    }
    keycloak_admin.create_user(no_roles_user)
