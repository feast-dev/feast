# RBAC demo
RBAC demo with local environment.

## System Requirements
* Clone of the Feast repo
* Docker
* yq

## Architecture
The demo creates the following components:
* An OIDC authorization server using a Keycloak docker container and initialized for demo purposes with a sample realm.
* A sample feature store using `feast init`, later adapted to use the `oidc` authorization against the sample realm.
* Three servers running the registry, online and offline stores.
* A client application connected to the servers to run test code.

## Setup the environment
Run the sample notebooks to setup the environment:
* [01.1-startkeycloak](./01.1-startkeycloak.ipynb) to start a Keycloak container.
* [01.2-setup-keycloak.ipynb](./01.2-setup-keycloak.ipynb) to configure Keycloak with all the needed resources for the next steps.
* [01.3-setup-feast.ipynb](./01.3-setup-feast.ipynb) to create the sample Feast store and inject the authoprization settings
* [02-registry_server.ipynb](./02-registry_server.ipynb) to start the Registry server
* [03-online_server.ipynb](./03-online_server.ipynb) to start the Online store server
* [04-offline_server.ipynb](04-offline_server.ipynb) to start the Offline store server

**Note**: For MacOs users, you must set this environment variable before launching the notebook server:
```bash
OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
```

## Goal
Once the environment is defined, we can use the [client.ipynb](./client.ipynb) notebook to verify how the behavior changes
according to the configured user.

In particular, given the configured permissions:
| Permission | Types | Name pattern | Actions | Roles |
|------------|-------|--------------|---------|-------|
| read_permission | ALL | | READ | reader |
| write_fresh_permission | FeatureView1 | .*_fresh | WRITE_ONLINE | fresh_writer |
| offline_permission | FeatureView, OnDemandFeatureView, FeatureService | | CRUD, WRITE_OFFLINE, QUERY_OFFLINE | batch_admin |
| admin_permission | ALL | | ALL | store_admin |

and the user roles defined in Keycloak:
| User | Roles |
|------|-------|
| reader | reader | 
| writer | fresh_writer |
| batch_admin | batch_admin |
| admin | store_admin |

We should expect the following behavior for each test section of the [client notebook](./client.ipynb):
| User | Basic validation | Historical | Materialization   | Online | Stream push |
|------|------------------|------------|-------------------|--------|-------------|
| reader | Ok             | Denied     | Denied            | Denied | Denied      | 
| writer | Empty          | Denied     | Ok                | Denied | Denied      |
| batch_admin | No Entities and Permissions | Ok | Denied  | Denied | Denied      |
| admin | Ok              | Ok         | Ok                | Ok     | Ok          |

