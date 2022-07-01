# Release process

## Release process

For Feast maintainers, these are the concrete steps for making a new release.

### Pre-release Verification (Verification that wheels are built correctly)

1. Merge upstream master changes into your fork.
2. Create a tag manually for the release on your fork. For example, if your release doing a release for version 0.22.0, create a tag by doing the following.
   - Checkout master branch and run `git tag v0.22.0`.
   - Run `git push --tags` to push the tag to remote.
3. Access the `Actions` tab on your github UI on your fork and click the `build_wheels` action.
4. Look for the header `This workflow has a workflow_dispatch event trigger` and click `Run Workflow` on the right.
5. Run the branch off of the tag you just created(`v0.22.0` in this case) and verify that the workflow worked.

### Release
1. Generate a [Personal Access Token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token) or retrieve your saved personal access token.
   - The personal access token should have all of the permissions under the `repo` checkbox.
2. Access the `Actions` tab on the main `feast-dev/feast` repo and find the `release` action.
3. Look for the header `This workflow has a workflow_dispatch event trigger` again and click `Run Workflow` on the right.
4. Try the dry run first with your personal access token. If this succeeds, uncheck `Dry Run` and run the release workflow.
5. All of the jobs should succeed besides the UI job which needs to be released separately. Ping a maintainer on Slack to run the UI release manually.
6. Try to install the feast release in your local environment and test out the `feast init` -> `feast apply` workflow to verify as a sanity check that the release worked correctly.
