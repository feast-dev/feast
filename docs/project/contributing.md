# Contribution process

## Getting started
After familiarizing yourself with the documentation, the simplest way to get started is to:
1. Setup your developer environment by following [development guide](development-guide.md). 
2. Either create a [GitHub issue](https://github.com/feast-dev/feast/issues) or make a draft PR (following [development guide](development-guide.md)) to get the ball rolling!

## Decision making process
*See [governance](../../community/governance.md) for more details here*

We follow a process of [lazy consensus](http://community.apache.org/committers/lazyConsensus.html). If you believe you know what the project needs then just start development. As long as there is no active opposition and the PR has been approved by maintainers or CODEOWNERS, contributions will be merged.

We use our [GitHub issues](https://github.com/feast-dev/feast/issues), and [GitHub pull requests](https://github.com/feast-dev/feast/pulls) to communicate development ideas.

> **Note**: There may not always a corresponding CODEOWNER for the affected code, in which case the responsibility falls on other maintainers or contributors with write access to review + merge the PR

## Pull requests

Please [submit a PR](https://github.com/feast-dev/feast/pulls) to the master branch of the Feast repository once you are ready to submit your contribution. Code submission to Feast \(including submission from project maintainers\) require review and approval from maintainers or code owners.

PRs that are submitted by the general public need to be identified as `ok-to-test`. Once enabled, [Prow](https://github.com/kubernetes/test-infra/tree/master/prow) will run a range of tests to verify the submission, after which community members will help to review the pull request.

See also [Making a pull request](development-guide.md#making-a-pull-request) for other guidelines on making pull requests in Feast.

## RFCs and Architecture Decision Records

For substantial changes (new features, architecture changes, removing features), we use an RFC process. See the [governance document](../../community/governance.md#rfcs-process) for details.

Once an RFC is finalized and approved, it should be recorded as an Architecture Decision Record (ADR) in the [`docs/adr/`](../adr/README.md) directory. This ensures that architectural decisions are version-controlled alongside the codebase and easily accessible to all contributors.

To add a finalized RFC as an ADR:

1. Copy the [ADR template](../adr/ADR-TEMPLATE.md) to a new file with the next sequential number.
2. Summarize the RFC's context, decision, and consequences.
3. Submit a pull request with the new ADR.

## Resources

- [Community](../community.md) for other ways to get involved with the community
- [Development guide](development-guide.md) for tips on how to contribute
- [Feast GitHub issues](https://github.com/feast-dev/feast/issues) to see what others are working on
- [Feast RFCs](https://drive.google.com/drive/u/0/folders/1msUsgmDbVBaysmhBlg9lklYLLTMk4bC3) for a folder of previously written RFCs
- [Architecture Decision Records](../adr/README.md) for documented architectural decisions