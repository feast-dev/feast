---
title: Feast Launches RBAC!
description: Feast now supports Role Based Access Controls (RBAC) so you can secure and govern your data with granular authorization policies.
date: 2024-11-21
authors: ["Danielle Martinoli", "Francisco Javier Arceo"]
---

<div class="hero-image">
  <img src="/images/blog/rbac-architecture.jpg" alt="Feast RBAC Architecture" loading="lazy">
</div>

# Feast Launches RBAC! 🚀

# What is the Feast Permission Model?

Feast now supports Role Based Access Controls (RBAC) so you can secure and govern your data. 

If you ever wanted to securely partition your feature store across different teams, the new Feast permissions model is here to make that possible!

This powerful feature allows administrators to configure granular authorization policies, letting them decide which users and groups can access specific resources and what operations they can perform.

The default implementation is based on Role-Based Access Control (RBAC): user roles determine whether a user has permission to perform specific functions on registered resources.

# Why is RBAC important for Feast?

Feature stores often operate on sensitive, proprietary data and we want to make sure teams are able to govern the access and control of that data thoughtfully, while benefiting from transparent code and an open source community like Feast.

That's why we built RBAC using [Kubernetes RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac) and [OpenID Connect protocol (OIDC)](https://auth0.com/docs/authenticate/protocols/openid-connect-protocol), ensuring secure, fine-grained access control in Feast.

# What are the Benefits of using Feast Permissions?

Using the Feast Permissions Model offers two key benefits:

1. Securely share and partition your feature store: grant each team only the minimum privileges necessary to access and manage the relevant resources.  
2. Adopt a Service-Oriented Architecture and leverage the benefits of a distributed system.

# How Feast Uses RBAC

## Permissions as Feast resources

The RBAC configuration is defined using a new Feast object type called "Permission". Permissions are registered in the Feast registry and are defined and applied like all the other registry objects, using Python code.

A permission is defined by these three components:

* A **resource**: a Feast object that we want to secure against unauthorized access. It's identified by the matching type(s), a possibly empty list of name patterns and a dictionary of required tags.  
* An **action**: a logical operation performed on the secured resource, such as managing the resource state with CREATE, DESCRIBE, UPDATE or DELETE, or accessing the resource data with READ and WRITE (differentiated by ONLINE and OFFLINE store types)  
* A **policy**: the rule to enforce authorization decisions based on the current user. The default implementation uses role-based policies.

The resource types supported by the permission framework are those defining the customer feature store:

* Project  
* Entity  
* FeatureView  
* OnDemandFeatureView  
* BatchFeatureView  
* StreamFeatureView  
* FeatureService  
* DataSource  
* ValidationReference  
* SavedDataset  
* Permission

   
**TIP**: Check out the Permission APIs in the [Feast Python API Documentation](https://docs.feast.dev/getting-started/concepts/permission) to learn more!

```python
# This configuration grants users with the 'owner' role permissions 
# to fetch resource status and read data from all the feature views 
from feast.permissions.action import AuthzedAction, READ
# Note: READ is a global list including both READ_OFFLINE and
# READ_ONLINE values from AuthzedAction enum

# You do not have to specify `name_patterns`
Permission(
   name="fv-owner",
   types=[FeatureView],
   policy=RoleBasedPolicy(roles=["owner"]),
   actions=[AuthzedAction.DESCRIBE, READ],
)

# This configuration grants users with the 'lab' role permissions 
# to fetch resource status and read data from all feature views 
# named 'lab_stream_feature_view' or 'lab_feature_view'
from feast.permissions.action import AuthzedAction, READ

Permission(
   name="lab-reader",
   types=[FeatureView],
   name_patterns=["lab_stream_feature_view", "lab_feature_view"],
   policy=RoleBasedPolicy(roles=["lab"]),
   actions=[AuthzedAction.DESCRIBE, READ],
)

# As an alternative, we can use Python regular expression patterns
# to grant the same permission to all feature views whose name
# starts by 'lab'
from feast.permissions.action import AuthzedAction, READ

Permission(
   name="lab-reader",
   types=[FeatureView],
   name_patterns="lab.*", # Accepts both 'str' and 'list[str]' types
   policy=RoleBasedPolicy(roles=["lab"]),
   actions=[AuthzedAction.DESCRIBE, READ],
)

# This configuration grants users with the 'prod' role permissions 
# to fetch resource status and read data from all feature views 
# whose names include the '_prod_' word
from feast.permissions.action import AuthzedAction, READ

Permission(
   name="prod-reader",
   types=[FeatureView, FeatureService],
   name_patterns=".*_prod_.*",
   policy=RoleBasedPolicy(roles=["prod"]),
   actions=[AuthzedAction.DESCRIBE, READ],
)

# This configuration grants permissions to write on all data sources 
# tagged with 'risk_level' set to 'high', exclusively to users 
# with the 'admin' or 'data_team' roles
from feast.permissions.action import WRITE
# Note: WRITE is a global list including both WRITE_OFFLINE and
# WRITE_ONLINE values from AuthzedAction enum 

Permission(
   name="data-writer",
   types=[DataSource],
   required_tags={"risk_level": "high"},
   policy=RoleBasedPolicy(roles=["admin", "data_team"]),
   actions=[WRITE],
)
```

## Why Now is the Time for Distributed Feature Stores

But wait a moment—does that mean every time I access the FeatureStore API, I have to go through an authorization check?

Well, yes and no-but mostly no if you work in a development environment.  
If your environment doesn't use any remote Feast service, RBAC enforcement won't take place.

Indeed, the reference architecture for the permission model feature represents a fully distributed environment:  

<div class="content-image">
  <img src="/images/blog/rbac-architecture.jpg" alt="RBAC Architecture Diagram" loading="lazy">
</div>

* Feast functions are deployed as interconnected services.  
* Service endpoints enforce authorization, processing only authorized requests.  
* Clients use the feature store transparently, with authorization headers automatically injected in every request.  
* Service-to-service communications are permitted automatically.

Currently, only the following Python servers are supported in an authorized environment:  
- Online REST feature server  
- Offline Arrow Flight feature server  
- gRPC Registry server

## Configuring Feast Authorization

For backward compatibility, by default no authorizations are enforced. The authorization functionality must be explicitly enabled using the auth configuration section in feature\_store.yaml.   
Of course, all server and client applications must have a consistent configuration.

Currently, feast supports [OIDC](https://auth0.com/docs/authenticate/protocols/openid-connect-protocol) and [Kubernetes RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac) authentication/authorization.

* With OIDC authorization, the client uses an OIDC server to fetch a JSON Web Token (JWT), which is then included in every request. On the server side, the token is parsed to extract user roles and validate them against the configured permissions.  
* With Kubernetes authorization, the client injects its service account JWT token into the request. The server then extracts the service account name from the token and uses it to look up the associated role in the Kubernetes RBAC resources.

## Inspecting and Troubleshooting the Permissions Model

The feast CLI includes a new `permissions` command to list the registered permissions, with options to identify the matching resources for each configured permission and the existing resources that are not covered by any permission.

For troubleshooting purposes, it also provides a command to list all the resources and operations allowed to any managed role.

# How Can I Get Started?

This new feature includes working examples for both supported authorization protocols. You can start by experimenting with these examples to see how they fit your own feature store and assess their benefits.

As this is a completely new functionality, your feedback will be extremely valuable. It will help us adapt the feature to meet real-world requirements and better serve our customers.
