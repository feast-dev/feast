# Development guide

## Overview

This guide is targeted at developers looking to contribute to Feast:

* [Project Structure](development-guide.md#repository-structure)
* [Making a Pull Request](development-guide.md#making-a-pull-request)
* [Feast Data Storage Format](development-guide.md#feast-data-storage-format)
* [Feast Protobuf API](development-guide.md#feast-protobuf-api)

> Learn How the Feast [Contributing Process](https://docs.feast.dev/contributing/contributing) works.

## Project Structure

Feast is composed of [multiple components](https://docs.feast.dev/v/master/concepts/architecture#components) distributed into multiple repositories:

<table>
  <thead>
    <tr>
      <th style="text-align:left">Repository</th>
      <th style="text-align:left">Description</th>
      <th style="text-align:left">Component(s)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left"><a href="https://github.com/feast-dev/feast">Main Feast Repository</a>
      </td>
      <td style="text-align:left">Hosts all required code to run Feast. This includes the Feast Python SDK
        and Protobuf definitions. For legacy reasons this repository still contains
        Terraform config and a Go Client for Feast.</td>
      <td style="text-align:left">
        <ul>
          <li><b>Python SDK / CLI</b>
          </li>
          <li><b>Protobuf APIs</b>
          </li>
          <li><b>Documentation</b>
          </li>
          <li><b>Go Client</b>
          </li>
          <li><b>Terraform</b>
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><a href="https://github.com/feast-dev/feast-java">Feast Java</a>
      </td>
      <td style="text-align:left">Java-specific Feast components. Includes the Feast Core Registry, Feast
        Serving for serving online feature values, and the Feast Java Client for
        retrieving feature values.</td>
      <td style="text-align:left">
        <ul>
          <li><b>Core</b>
          </li>
          <li><b>Serving</b>
          </li>
          <li><b>Java Client</b>
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><a href="https://github.com/feast-dev/feast-spark">Feast Spark</a>
      </td>
      <td style="text-align:left">Feast Spark SDK &amp; Feast Job Service for launching ingestion jobs and
        for building training datasets with Spark</td>
      <td style="text-align:left">
        <ul>
          <li><b>Spark SDK</b>
          </li>
          <li><b>Job Service</b>
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td style="text-align:left"><a href="https://github.com/feast-dev/feast-helm-charts/">Feast Helm Chart</a>
      </td>
      <td style="text-align:left">Helm Chart for deploying Feast on Kubernetes &amp; Spark.</td>
      <td style="text-align:left">
        <ul>
          <li><b>Helm Chart</b>
          </li>
        </ul>
      </td>
    </tr>
  </tbody>
</table>

## Making a Pull Request

#### Incorporating upstream changes from master

Our preference is the use of `git rebase` instead of `git merge` : `git pull -r`

#### Signing commits

Commits have to be signed before they are allowed to be merged into the Feast codebase:

```bash
# Include -s flag to signoff
git commit -s -m "My first commit"
```

#### Good practices to keep in mind

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

#### Generating Language Bindings

The language specific bindings have to be regenerated when changes are made to the Feast Protobuf API:

| Repository | Language | Regenerating Language Bindings |
| :--- | :--- | :--- |
| [Main Feast Repository](https://github.com/feast-dev/feast) | Python | Run `make compile-protos-python` to generate bindings |
| [Main Feast Repository](https://github.com/feast-dev/feast) | Golang | Run `make compile-protos-go` to generate bindings |
| [Feast Java](https://github.com/feast-dev/feast-java) | Java | No action required: bindings are generated automatically during compilation. |

#### 

