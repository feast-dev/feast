<p align="center">
    <a href="https://feast.dev/">
      <img src="docs/assets/feast_logo.png" width="550">
    </a>
</p>
<br />

[![Unit Tests](https://github.com/feast-dev/feast/workflows/unit%20tests/badge.svg?branch=master)](https://github.com/feast-dev/feast/actions?query=workflow%3A%22unit+tests%22+branch%3Amaster)
[![Code Standards](https://github.com/feast-dev/feast/workflows/code%20standards/badge.svg?branch=master)](https://github.com/feast-dev/feast/actions?query=workflow%3A%22code+standards%22+branch%3Amaster)
[![Docs Latest](https://img.shields.io/badge/docs-latest-blue.svg)](https://docs.feast.dev/)
[![GitHub Release](https://img.shields.io/github/v/release/feast-dev/feast.svg?style=flat&sort=semver&color=blue)](https://github.com/feast-dev/feast/releases)

## Overview

Feast (Feature Store) is an operational data system for managing and serving machine learning features to models in production. Please see our [documentation](https://docs.feast.dev/) for the motivation behind the project.

![](docs/.gitbook/assets/feast-architecture-diagrams.svg)

## Getting Started with Docker Compose

Clone the latest stable version of the [Feast repository](https://github.com/feast-dev/feast/) and navigate to the `infra/docker-compose` sub-directory:

```
git clone --depth 1 --branch v0.7.0 https://github.com/feast-dev/feast.git
cd feast/infra/docker-compose
cp .env.sample .env
```

The `.env` file can optionally be configured based on your environment.

Bring up Feast:
```
docker-compose up -d
```

The command above will bring up a complete Feast deployment with a [Jupyter Notebook](http://localhost:8888/tree/feast/examples) containing example notebooks.

## Important resources

Please refer to the official documentation at <https://docs.feast.dev>

 * [Concepts](https://docs.feast.dev/user-guide/overview)
 * [Installation](https://docs.feast.dev/getting-started)
 * [Examples](https://github.com/feast-dev/feast/blob/master/examples/)
 * [Roadmap](https://docs.feast.dev/roadmap)
 * [Change Log](https://github.com/feast-dev/feast/blob/master/CHANGELOG.md)
 * [Slack (#Feast)](https://join.slack.com/t/kubeflow/shared_invite/zt-cpr020z4-PfcAue_2nw67~iIDy7maAQ)

## Notice

Feast is a community project and is still under active development. Your feedback and contributions are important to us. Please have a look at our [contributing guide](docs/contributing/contributing.md) for details.
