# Release Process

Feast uses [semantic versioning](https://semver.org/).

* Major and minor releases are cut from the `master` branch.
* Whenever a major or minor release is cut, a branch is created for that release. This is called a "release branch". For example if `0.3` is released from `master`, a branch named `v0.3-branch` is created.
* You can create a release branch via the GitHub UI.
* From this branch a git tag is created for the specific release, for example `v0.3.0`.
* Tagging a release will automatically build and push the relevant artifacts to their repositories or package managers \(docker images, Python wheels, etc\).
* A release branch should be substantially _feature complete_ with respect to the intended release. Code that is committed to `master` may be merged or cherry-picked on to a release branch, but code that is directly committed to the release branch should be solely applicable to that release \(and should not be committed back to master\).
* In general, unless you're committing code that only applies to the release stream \(for example, temporary hotfixes, backported security fixes, or image hashes\), you should commit to `master` and then merge or cherry-pick to the release branch.
* It is also important to update the [CHANGELOG.md](https://github.com/gojek/feast/blob/master/CHANGELOG.md) when submitting a new release. This can be in the same PR or a separate PR.
* Finally it is also important to create a [GitHub release](https://github.com/gojek/feast/releases) which includes a summary of important changes as well as any artifacts associated with that release.

