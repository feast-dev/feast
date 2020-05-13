# Releasing Feast

## Versioning policy and branch workflow

Feast uses [semantic versioning](https://semver.org/). As such, while it is still pre-1.0 breaking changes will happen in minor versions.

Contributors are encouraged to understand our branch workflow described below, for choosing where to branch when making a change (and thus the merge base for a pull request).

* Major and minor releases are cut from the `master` branch.
* Each major and minor release has a long-lived maintenance branch, for example `v0.3-branch`. This is called a "release branch".
* From the release branches, patch version releases are tagged, for example `v0.3.0`.

A release branch should be substantially _feature complete_ with respect to the intended release. Code that is committed to `master` may be merged or cherry-picked on to a release branch, but code that is directly committed to a release branch should be solely applicable to that release \(and should not be committed back to master\).

In general, unless you're committing code that only applies to a particular release stream \(for example, temporary hotfixes, backported security fixes, or image hashes\), you should base changes from `master` and then merge or cherry-pick to the release branch.

## Release process

For Feast maintainers, these are the concrete steps for making a new release.

1. For a major or minor release, create and check out the release branch for the new stream, e.g. `v0.6-branch`. For a patch version, check out the stream's release branch.
1. Update the [CHANGELOG.md]. See the [Creating a change log](#creating-a-change-log) guide.
1. In the root `pom.xml`, remove `-SNAPSHOT` from the `<revision>` property, and commit.
1. Push. For a new release branch, open a PR against master.
1. When CI passes, merge. (Remember _not_ to delete the new release branch).
1. Tag the merge commit with the release version, using a `v` prefix. Push the tag.
1. Bump to the next working version and append `-SNAPSHOT` in `pom.xml`.
1. Commit the POM and open a PR.
1. Create a [GitHub release](https://github.com/gojek/feast/releases) which includes a summary of important changes as well as any artifacts associated with the release. Make sure to include the same change log as added in [CHANGELOG.md]. Use `Feast vX.Y.Z` as the title.
1. Create one final PR to the master branch and also update its [CHANGELOG.md].

When a tag that matches a Semantic Version string is pushed, CI will automatically build and push the relevant artifacts to their repositories or package managers \(docker images, Python wheels, etc\). JVM artifacts are promoted from Sonatype OSSRH to Maven Central, but it sometimes takes some time for them to be available.

[CHANGELOG.md]: https://github.com/gojek/feast/blob/master/CHANGELOG.md

### Creating a change log

We use an [open source change log generator](https://hub.docker.com/r/ferrarimarco/github-changelog-generator/) to generate change logs. The process still requires a little bit of manual effort.
1. Create a GitHub token as [per these instructions ](https://github.com/github-changelog-generator/github-changelog-generator#github-token). The token is used as an input argument (`-t`) to the changelog generator.
2. The change log generator configuration below will look for unreleased changes on a specific branch. The branch will be `master` for a major/minor release, or a release branch (`v0.4-branch`) for a patch release. You will need to set the branch using the `--release-branch` argument.
3. You should also set the `--future-release` argument. This is the version you are releasing. The version can still be changed at a later date.
4. Update the  arguments below and run the command to generate the change log to the console.
```
docker run -it --rm ferrarimarco/github-changelog-generator \
--user gojek \
--project feast  \
--release-branch <release-branch-to-find-changes>  \
--future-release <proposed-release-version>  \
--unreleased-only  \
--no-issues  \
--bug-labels kind/bug  \
--enhancement-labels kind/feature  \
--breaking-labels compat/breaking  \
-t <your-github-token>  \
--max-issues 1 \
-o
```
5. Review each change log item.
    - Make sure that sentences are grammatically correct and well formatted (although we will try to enforce this at the PR review stage). 
    - Make sure that each item is categorized correctly. You will see the following categories: `Breaking changes`, `Implemented enhancements`, `Fixed bugs`, and `Merged pull requests`. Any unlabeled PRs will be found in `Merged pull requests`. It's important to make sure that any `breaking changes`, `enhancements`, or `bug fixes` are pulled up out of `merged pull requests` into the correct category. Housekeeping, tech debt clearing, infra changes, or refactoring do not count as `enhancements`. Only enhancements a user benefits from should be listed in that category.
    - Make sure that the "Full Changelog" link is actually comparing the correct tags (normally your released version against the previously version).
    - Make sure that release notes and breaking changes are present.
