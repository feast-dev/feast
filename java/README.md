# Feast Java components

### Overview

This repository contains the following Feast components.
* Feast Serving: A service used to serve the latest feature values to models.
* Feast Java SDK: A client used to retrieve features from Feast Serving.

### Architecture

Feast Serving has a dependency on an online store (Redis) for retrieving features. 
The process of ingesting data into the online store (Redis) is decoupled from the process of reading from it.

### Contributing
Guides on Contributing:
- [Contribution Process for Feast](https://docs.feast.dev/v/master/contributing/contributing)
- [Development Guide for Feast](https://docs.feast.dev/contributing/development-guide)
- [Development Guide for feast-java (this repository)](CONTRIBUTING.md)

### Installing using Helm
Please see the Helm charts in [infra/charts/feast](../infra/charts/feast).
