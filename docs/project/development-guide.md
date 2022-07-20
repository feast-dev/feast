# Development guide

## Overview

This guide is targeted at developers looking to contribute to Feast:

* [Project Structure](development-guide.md#repository-structure)
* [Making a Pull Request](development-guide.md#making-a-pull-request)
* [Feast Data Storage Format](development-guide.md#feast-data-storage-format)
* [Feast Protobuf API](development-guide.md#feast-protobuf-api)

> Learn How the Feast [Contributing Process](contributing.md) works.

## Making a Pull Request

{% hint style="info" %}
See also the CONTRIBUTING.md in the corresponding GitHub repository \(e.g. [main repo doc](https://github.com/feast-dev/feast/blob/master/CONTRIBUTING.md)\)
{% endhint %}

### Incorporating upstream changes from master

Our preference is the use of `git rebase` instead of `git merge` : `git pull -r`

### Signing commits

Commits have to be signed before they are allowed to be merged into the Feast codebase:

```bash
# Include -s flag to signoff
git commit -s -m "My first commit"
```

### Good practices to keep in mind

* Fill in the description based on the default template configured when you first open the PR
  * What this PR does/why we need it
  * Which issue\(s\) this PR fixes
  * Does this PR introduce a user-facing change
* Include `kind` label when opening the PR
* Add `WIP:` to PR name if more work needs to be done prior to review
* Avoid `force-pushing` as it makes reviewing difficult

**Managing CI-test failures**

* GitHub runner tests
  * Click `checks` tab to analyse failed tests
* Prow tests
  * Visit [Prow status page ](http://prow.feast.ai/)to analyse failed tests

## Feast Data Storage Format

Feast data storage contracts are documented in the following locations:

* [Feast Offline Storage Format](https://github.com/feast-dev/feast/blob/master/docs/specs/offline_store_format.md): Used by BigQuery, Snowflake \(Future\), Redshift \(Future\).
* [Feast Online Storage Format](https://github.com/feast-dev/feast/blob/master/docs/specs/online_store_format.md): Used by Redis, Google Datastore.

## Feast Protobuf API

Feast Protobuf API defines the common API used by Feast's Components:

* Feast Protobuf API specifications are written in [proto3](https://developers.google.com/protocol-buffers/docs/proto3) in the Main Feast Repository.
* Changes to the API should be proposed via a [GitHub Issue](https://github.com/feast-dev/feast/issues/new/choose) for discussion first.

### Generating Language Bindings

The language specific bindings have to be regenerated when changes are made to the Feast Protobuf API:

| Repository | Language | Regenerating Language Bindings |
| :--- | :--- | :--- |
| [Main Feast Repository](https://github.com/feast-dev/feast) | Python | Run `make compile-protos-python` to generate bindings |
| [Main Feast Repository](https://github.com/feast-dev/feast) | Golang | Run `make compile-protos-go` to generate bindings |
