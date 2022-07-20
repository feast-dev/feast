---
description: Versioning policies and status of Feast components
---

# Versioning policy

## Versioning policy and branch workflow

Feast uses [semantic versioning](https://semver.org/).

Contributors are encouraged to understand our branch workflow described below, for choosing where to branch when making a change \(and thus the merge base for a pull request\).

* Major and minor releases are cut from the `master` branch.
* Each major and minor release has a long-lived maintenance branch, e.g., `v0.3-branch`. This is called a "release branch".
* From the release branch the pre-release release candidates are tagged, e.g., `v0.3.0-rc.1`
* From the release candidates the stable patch version releases are tagged, e.g.,`v0.3.0`.

A release branch should be substantially _feature complete_ with respect to the intended release. Code that is committed to `master` may be merged or cherry-picked on to a release branch, but code that is directly committed to a release branch should be solely applicable to that release \(and should not be committed back to master\).

In general, unless you're committing code that only applies to a particular release stream \(for example, temporary hot-fixes, back-ported security fixes, or image hashes\), you should base changes from `master` and then merge or cherry-pick to the release branch.

## Feast Component Matrix

The following table shows the **status** \(stable, beta, or alpha\) of Feast components.

Component status indicators for Feast:

* **Stable** means that the component has reached a sufficient level of stability and adoption that the Feast community has deemed the component stable. Please see the stability criteria below.
* **Beta** means that the component is working towards a version 1.0 release. Beta does not mean a component is unstable, it simply means the component has not met the full criteria of stability.
* **Alpha** means that the component is in the early phases of development and/or integration into Feast.

| Component                                                                        | Status | Notes |
|:---------------------------------------------------------------------------------|:-------| :--- |
| [Feast Python SDK](https://github.com/feast-dev/feast/tree/master/sdk/python)    | Stable |  |
| [Feast Go Feature Server](https://github.com/feast-dev/feast/tree/master/)       | Beta   |  |
| [Feast Java Feature Server](https://github.com/feast-dev/feast/tree/master/java) | Alpha  |  |
|                                                                                  |        |  |

Criteria for reaching _**stable**_ status:

* Contributors from at least two organizations
* Complete end-to-end test suite
* Scalability and load testing if applicable
* Automated release process \(docker images, PyPI packages, etc\)
* API reference documentation
* No deprecative changes
* Must include logging and monitoring

Criteria for reaching **beta** status

* Contributors from at least two organizations
* End-to-end test suite
* API reference documentation
* Deprecative changes must span multiple minor versions and allow for an upgrade path.

## Levels of support <a id="levels-of-support"></a>

Feast components have various levels of support based on the component status.

| Application status | Level of support |
| :--- | :--- |
| Stable | The Feast community offers best-effort support for stable applications. Stable components will be offered long term support |
| Beta | The Feast community offers best-effort support for beta applications. Beta applications will be supported for at least 2 more minor releases. |
| Alpha | The response differs per application in alpha status, depending on the size of the community for that application and the current level of active development of the application. |

## Support from the Feast community <a id="support-from-the-kubeflow-community"></a>

Feast has an active and helpful community of users and contributors.

The Feast community offers support on a best-effort basis for stable and beta applications. Best-effort support means that there’s no formal agreement or commitment to solve a problem but the community appreciates the importance of addressing the problem as soon as possible. The community commits to helping you diagnose and address the problem if all the following are true:

* The cause falls within the technical framework that Feast controls. For example, the Feast community may not be able to help if the problem is caused by a specific network configuration within your organization.
* Community members can reproduce the problem.
* The reporter of the problem can help with further diagnosis and troubleshooting.

Please see the [Community](../community.md) page for channels through which support can be requested.

