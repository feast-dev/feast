# API Compatibility in Feast

Feast follows [semantic versions](./versioning-policy.md). Being pre-1.0, Feast is still considered in active initial development as per the [SemVer spec](https://semver.org/#faq).

That being said, Feast takes backwards compatibility seriously, to ensure that introducing new functionality across minor versions does not break current users.

When possible, API changes should always be made in a backwards compatible way. 
If this is not possible, the maintainers introduce new APIs alongside existing, now-deprecated APIs, with the intention of supporting the existing APIs for at least 3 minor versions, before deprecating and removing them.
In some cases, the deprecated APIs may be supported for longer than 3 minor versions, if necessary to give users a longer time for migrations.

When deprecating existing APIs, deprecation warnings should be introduced early, with the expected version at which the deprecated API would be removed. 

At this point, core functionality in Feast is considered "stable" and ready for usage. However, there are still some components that are considered "Alpha". Please check the [roadmap](../roadmap.md) for a full list of all capabilities and their status.
