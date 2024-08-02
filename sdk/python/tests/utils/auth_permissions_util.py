import os
import subprocess

import yaml
from keycloak import KeycloakAdmin

from feast import (
    Entity,
    FeatureStore,
    FeatureView,
    OnDemandFeatureView,
    RepoConfig,
    StreamFeatureView,
)
from feast.infra.registry.remote import RemoteRegistryConfig
from feast.permissions.action import AuthzedAction
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.wait import wait_retry_backoff
from tests.utils.cli_repo_creator import CliRunner
from tests.utils.http_server import check_port_open

PROJECT_NAME = "feast_test_project"

list_permissions_perm = Permission(
    name="list_permissions_perm",
    types=Permission,
    policy=RoleBasedPolicy(roles=["reader"]),
    actions=[AuthzedAction.READ, AuthzedAction.QUERY_OFFLINE],
)

list_entities_perm = Permission(
    name="list_entities_perm",
    types=Entity,
    with_subclasses=False,
    policy=RoleBasedPolicy(roles=["reader"]),
    actions=[AuthzedAction.READ, AuthzedAction.QUERY_OFFLINE],
)

list_fv_perm = Permission(
    name="list_fv_perm",
    types=FeatureView,
    with_subclasses=False,
    policy=RoleBasedPolicy(roles=["reader"]),
    actions=[AuthzedAction.READ, AuthzedAction.QUERY_OFFLINE],
)

list_odfv_perm = Permission(
    name="list_odfv_perm",
    types=OnDemandFeatureView,
    with_subclasses=False,
    policy=RoleBasedPolicy(roles=["reader"]),
    actions=[AuthzedAction.READ, AuthzedAction.QUERY_OFFLINE],
)

list_sfv_perm = Permission(
    name="list_sfv_perm",
    types=StreamFeatureView,
    with_subclasses=False,
    policy=RoleBasedPolicy(roles=["reader"]),
    actions=[AuthzedAction.READ, AuthzedAction.QUERY_OFFLINE],
)

invalid_list_entities_perm = Permission(
    name="invalid_list_entity_perm",
    types=Entity,
    with_subclasses=False,
    policy=RoleBasedPolicy(roles=["dancer"]),
    actions=[AuthzedAction.READ, AuthzedAction.QUERY_OFFLINE],
)


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
    result = runner.run(["init", PROJECT_NAME], cwd=temp_dir)
    repo_path = os.path.join(temp_dir, PROJECT_NAME, "feature_repo")
    assert result.returncode == 0

    include_auth_config(
        file_path=f"{repo_path}/feature_store.yaml", auth_config=auth_config
    )

    result = runner.run(["--chdir", repo_path, "apply"], cwd=temp_dir)
    assert result.returncode == 0

    fs = FeatureStore(repo_path=repo_path)

    fs.apply(permissions)

    return fs


def start_feature_server(repo_path: str, server_port: int):
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

    yield f"http://localhost:{server_port}"

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


def get_remote_registry_store(server_port, feature_store):
    registry_config = RemoteRegistryConfig(
        registry_type="remote", path=f"localhost:{server_port}"
    )

    store = FeatureStore(
        config=RepoConfig(
            project=PROJECT_NAME,
            auth=feature_store.config.auth,
            registry=registry_config,
            provider="local",
            entity_key_serialization_version=2,
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
