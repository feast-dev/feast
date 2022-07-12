# Release process

## Release process

For Feast maintainers, these are the concrete steps for making a new release.

### Pre-release Verification (Verification that wheels are built correctly) for minor release.
1. Merge upstream master changes into your **fork**. Make sure you are running the workflow off of your fork!
2. Create a tag manually for the release on your fork. For example, if you are doing a release for version 0.22.0, create a tag by doing the following.
   - Checkout master branch and run `git tag v0.22.0`.
   - Run `git push --tags` to push the tag to your forks master branch.
3. Access the `Actions` tab on your github UI on your fork and click the `build_wheels` action. This workflow will build the python sdk wheels for Python 3.8-3.10 on MacOS 10.15 and Linux and verify that these wheels are correct. The publish workflow uses this action to publish the python wheels for a new release to pypi.
4. Look for the header `This workflow has a workflow_dispatch event trigger` and click `Run Workflow` on the right.
5. Run the workflow off of the tag you just created(`v0.22.0` in this case) and verify that the workflow worked (i.e ensure that all jobs are green).

### Pre-release Verification (Verification that wheels are built correctly) for patch release.
1. Check out the branch of your release (e.g `v0.22-branch` on your local **fork**) and push this to your fork (`git push -u origin <branch>`).
2. Cherry pick commits that are relevant to the patch release onto your forked branch.
3. Checkout the release branch and add a patch release tag (e.g `v0.22.1`) by running `git tag <tag>`.
4. Push tags to your origin branch with `git push origin <tag>`.
5. Kick off `build_wheels` workflow in the same way as is detailed in the last section on of the patch release tag.

### Release for Python and Java SDK
1. Generate a [Personal Access Token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token) or retrieve your saved personal access token.
   - The personal access token should have all of the permissions under the `repo` checkbox.
2. Access the `Actions` tab on the main `feast-dev/feast` repo and find the `release` action.
3. Look for the header `This workflow has a workflow_dispatch event trigger` again and click `Run Workflow` on the right.
4. Try the dry run first with your personal access token. If this succeeds, uncheck `Dry Run` and run the release workflow.
5. All of the jobs should succeed besides the UI job which needs to be released separately. Ping a maintainer on Slack to run the UI release manually.
6. Try to install the feast release in your local environment and test out the `feast init` -> `feast apply` workflow to verify as a sanity check that the release worked correctly.
