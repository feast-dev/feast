# Release process

## Release process

For Feast maintainers, these are the concrete steps for making a new release.

1. For new major or minor release, create and check out the release branch for the new stream, e.g. `v0.6-branch`. For a patch version, check out the stream's release branch.
2. Update the [CHANGELOG.md](https://github.com/feast-dev/feast/blob/master/CHANGELOG.md). See the [Creating a change log](release-process.md#creating-a-change-log) guide and commit
   * Make to review each PR in the changelog to [flag any breaking changes and deprecation.](release-process.md#flag-breaking-changes-and-deprecations)
3. Update versions for the release/release candidate with a commit:
   1. In the root `pom.xml`, remove `-SNAPSHOT` from the `<revision>` property,  update versions, and commit.
   2. Tag the commit with the release version, using a `v` and `sdk/go/v` prefixes 
      * for a release candidate, create tags `vX.Y.Z-rc.N`and `sdk/go/vX.Y.Z-rc.N`
      * for a stable release `X.Y.Z` create tags `vX.Y.Z` and `sdk/go/vX.Y.Z`
   3. Check that versions are updated with `make lint-versions`.
   4. If changes required are flagged by the version lint, make the changes, amend the commit and move the tag to the new commit.
4. Push the commits and tags. Make sure the CI passes.
   * If the CI does not pass, or if there are new patches for the release fix, repeat step 2 & 3 with release candidates until stable release is achieved.
5. Bump to the next patch version in the release branch, append `-SNAPSHOT` in `pom.xml` and push.
6. Create a PR against master to:
   1. Bump to the next major/minor version and  append `-SNAPSHOT` .
   2. Add the change log by applying the change log commit created in step 2.
   3. Check that versions are updated with `env TARGET_MERGE_BRANCH=master make lint-versions`
7. Create a [GitHub release](https://github.com/feast-dev/feast/releases) which includes a summary of im~~p~~ortant changes as well as any artifacts associated with the release. Make sure to include the same change log as added in [CHANGELOG.md](https://github.com/feast-dev/feast/blob/master/CHANGELOG.md). Use `Feast vX.Y.Z` as the title.
8. Update the[ Upgrade Guide](../feast-on-kubernetes-1/advanced-1/upgrading.md)  to include the action required instructions for users to upgrade to this new release. Instructions should include a migration for each breaking change made to this release.

When a tag that matches a Semantic Version string is pushed, CI will automatically build and push the relevant artifacts to their repositories or package managers \(docker images, Python wheels, etc\). JVM artifacts are promoted from Sonatype OSSRH to Maven Central, but it sometimes takes some time for them to be available. The `sdk/go/v tag` is required to version the Go SDK go module so that users can go get a specific tagged release of the Go SDK.

### Creating a change log

We use an [open source change log generator](https://hub.docker.com/r/ferrarimarco/github-changelog-generator/) to generate change logs. The process still requires a little bit of manual effort. 

1. Create a GitHub token as [per these instructions](https://github.com/github-changelog-generator/github-changelog-generator#github-token). The token is used as an input argument \(`-t`\) to the change log generator. 
2. The change log generator configuration below will look for unreleased changes on a specific branch. The branch will be `master` for a major/minor release, or a release branch \(`v0.4-branch`\) for a patch release. You will need to set the branch using the `--release-branch` argument.
3. You should also set the `--future-release` argument. This is the version you are releasing. The version can still be changed at a later date. 
4. Update the arguments below and run the command to generate the change log to the console.

```text
docker run -it --rm ferrarimarco/github-changelog-generator \
--user feast-dev \
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

1. Review each change log item.
   * Make sure that sentences are grammatically correct and well formatted \(although we will try to enforce this at the PR review stage\). 
   * Make sure that each item is categorised correctly. You will see the following categories: `Breaking changes`, `Implemented enhancements`, `Fixed bugs`, and `Merged pull requests`. Any unlabelled PRs will be found in `Merged pull requests`. It's important to make sure that any `breaking changes`, `enhancements`, or `bug fixes` are pulled up out of `merged pull requests` into the correct category. Housekeeping, tech debt clearing, infra changes, or refactoring do not count as `enhancements`. Only enhancements a user benefits from should be listed in that category.
   * Make sure that the "Full Change log" link is actually comparing the correct tags \(normally your released version against the previously version\).
   * Make sure that release notes and breaking changes are present.

### Flag Breaking Changes & Deprecations

It's important to flag breaking changes and deprecation to the API for each release so that we can maintain API compatibility.

Developers should have flagged PRs with breaking changes with the `compat/breaking` label. However, it's important to double check each PR's release notes and contents for changes that will break API compatibility and manually label `compat/breaking` to PRs with undeclared breaking changes. The change log will have to be regenerated if any new labels have to be  added.

