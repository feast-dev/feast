# Contribution process

## Getting started
After familiarizing yourself with the documentation, the simplest way to get started is to:
1. Join the `#feast-development` [Slack channel](https://tectonfeast.slack.com/archives/C01NTDB88QK), where contributors discuss ideas and PRs
2. Join our Google Groups in order to get access to RFC folders + get invites to community calls. See [community](../community.md) for more details.
3. Setup your developer environment by following [development guide](development-guide.md). 
4. Either create a [GitHub issue](https://github.com/feast-dev/feast/issues) or make a draft PR (following [development guide](development-guide.md)) to get the ball rolling!

## Decision making process
*See [governance](../../community/governance.md) for more details here*

We follow a process of [lazy consensus](http://community.apache.org/committers/lazyConsensus.html). If you believe you know what the project needs then just start development. As long as there is no active opposition and the PR has been approved by maintainers or CODEOWNERS, contributions will be merged.

We use our `#feast-development` [Slack channel](https://tectonfeast.slack.com/archives/C01NTDB88QK), [GitHub issues](https://github.com/feast-dev/feast/issues), and [GitHub pull requests](https://github.com/feast-dev/feast/pulls) to communicate development ideas.

The general decision making workflow is as follows:

<img src="../../community/governance.png" width=600></img>

> **Note**: There may not always a corresponding CODEOWNER for the affected code, in which case the responsibility falls on other maintainers or contributors with write access to review + merge the PR

## Pull requests

Please [submit a PR](https://github.com/feast-dev/feast/pulls) to the master branch of the Feast repository once you are ready to submit your contribution. Code submission to Feast \(including submission from project maintainers\) require review and approval from maintainers or code owners.

PRs that are submitted by the general public need to be identified as `ok-to-test`. Once enabled, [Prow](https://github.com/kubernetes/test-infra/tree/master/prow) will run a range of tests to verify the submission, after which community members will help to review the pull request.

See also [Making a pull request](development-guide.md#making-a-pull-request) for other guidelines on making pull requests in Feast.

## Resources

- [Community](../community.md) for other ways to get involved with the community (e.g. joining community calls)
- [Development guide](development-guide.md) for tips on how to contribute
- [Feast GitHub issues](https://github.com/feast-dev/feast/issues) to see what others are working on
- [Feast RFCs](https://drive.google.com/drive/u/0/folders/1msUsgmDbVBaysmhBlg9lklYLLTMk4bC3) for a folder of previously written RFCs