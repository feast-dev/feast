<p align="center">
    <a href="https://feast.dev/">
      <img src="docs/assets/feast_logo.png" width="550">
    </a>
</p>
<br />

[![unit-tests](https://github.com/feast-dev/feast/actions/workflows/unit_tests.yml/badge.svg?branch=master&event=push)](https://github.com/feast-dev/feast/actions/workflows/unit_tests.yml)
[![integration-tests](https://github.com/feast-dev/feast/actions/workflows/integration_tests.yml/badge.svg?branch=master&event=push)](https://github.com/feast-dev/feast/actions/workflows/integration_tests.yml)
[![linter](https://github.com/feast-dev/feast/actions/workflows/linter.yml/badge.svg?branch=master&event=push)](https://github.com/feast-dev/feast/actions/workflows/linter.yml)
[![Docs Latest](https://img.shields.io/badge/docs-latest-blue.svg)](https://docs.feast.dev/)
[![Python API](https://img.shields.io/readthedocs/feast/master?label=Python%20API)](http://rtd.feast.dev/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue)](https://github.com/feast-dev/feast/blob/master/LICENSE)
[![GitHub Release](https://img.shields.io/github/v/release/feast-dev/feast.svg?style=flat&sort=semver&color=blue)](https://github.com/feast-dev/feast/releases)

## Overview

Feast (Feature Store) is an operational data system for managing and serving machine learning features to models in production. Please see our [documentation](https://docs.feast.dev/) for more information about the project.

![](docs/architecture.png)

## Getting Started with Docker Compose

Clone the latest stable version of the [Feast repository](https://github.com/feast-dev/feast/) and navigate to the `infra/docker-compose` sub-directory:

```
git clone https://github.com/feast-dev/feast.git
cd feast/infra/docker-compose
cp .env.sample .env
```

The `.env` file can optionally be configured based on your environment.

Bring up Feast:
```
docker-compose pull && docker-compose up -d
```
Please wait for the containers to start up. This could take a few minutes since the quickstart contains demo infastructure like Kafka and Jupyter.

Once the containers are all running, please connect to the provided [Jupyter Notebook](http://localhost:8888/tree/minimal) containing example notebooks to try out.

## Important resources

Please refer to the official documentation at <https://docs.feast.dev>

 * [Concepts](https://docs.feast.dev/concepts/overview)
 * [Installation](https://docs.feast.dev/getting-started)
 * [Examples](https://github.com/feast-dev/feast/blob/master/examples/)
 * [Roadmap](https://docs.feast.dev/roadmap)
 * [Change Log](https://github.com/feast-dev/feast/blob/master/CHANGELOG.md)
 * [Slack (#Feast)](https://join.slack.com/t/tectonfeast/shared_invite/zt-n7pl8gnb-H7dLlH9yQsgbchOp36ZUxQ)

## Contributing
Feast is a community project and is still under active development.Your feedback and contributions are important to us. Please have a look at our contributing and development guides:
- [Contribution Process for Feast](https://docs.feast.dev/v/master/contributing/contributing)
- [Development Guide for Feast](https://docs.feast.dev/contributing/development-guide)
- [Development Guide for the Main Feast Repository](./CONTRIBUTING.md)

## Contributors ✨

Thanks goes to these incredible people:

<a href="https://github.com/feast-dev/feast/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=feast-dev/feast" />
</a>

