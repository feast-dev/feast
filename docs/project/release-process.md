# Release process

## Release process

For Feast maintainers, these are the concrete steps for making a new release.

### Pre-release Verification

1. Merge upstream master changes into your fork.
2. Create a tag manually for the release on your fork. For example, if your release is on version 0.22.0, create a tag by doing the following.
   - Checkout master branch and run `git tag v0.22.0`.
   - Run `git push --tags` to push the tag to remote.
3. Access the `Actions` tab on your github UI on your fork and click the `build_wheels` action.
4. Look for the header `This workflow has a workflow_dispatch event trigger.` and click `Run Workflow` on the right.
5. Run the branch off of the tag you just created(`v0.22.0` in this case) and verify that the workflow worked.

### Release
6. Generate a [Personal Access Token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token) or retrieve your saved personal access token.
   - The personal access token should have all of the permissions under the `repo` checkbox.
7. Access the `Actions` tab on the main `feast-dev/feast` repo and find the `release` action.
8. Look for the header `This workflow has a workflow_dispatch event trigger.` again and click `Run Workflow` on the right.
9. Try the dry run first with your personal access token. If this succeeds, uncheck `Dry Run` and run the release workflow.
10. All of the jobs should succeed besides the UI job which needs to be released separately. Ping a maintainer on Slack to run the UI release manually.

### Flag Breaking Changes & Deprecations

It's important to flag breaking changes and deprecation to the API for each release so that we can maintain API compatibility.

Developers should have flagged PRs with breaking changes with the `compat/breaking` label. However, it's important to double check each PR's release notes and contents for changes that will break API compatibility and manually label `compat/breaking` to PRs with undeclared breaking changes.