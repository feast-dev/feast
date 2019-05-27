# Feast - Feature Store for Machine Learning [![Build Status](http://prow.feast.ai/badge.svg?jobs=integration-test)](http://prow.feast.ai)

## Overview

Feast (Feature Store) is a tool to manage storage and access of machine learning features.

It aims to:
* Support ingesting feature data via batch or streaming
* Provide scalable storage of feature data for serving and training
* Provide an API for low latency access of features
* Enable discovery and documentation of features
* Provide an overview of the general health of features in the system

## High Level Architecture

![Feast Architecture](docs/architecture.png)

The Feast platform is broken down into the following functional areas:

* __Create__ features based on defined format and programming model
* __Ingest__ features via streaming input, import from files or BigQuery tables, and write to an appropriate data store
* __Store__ feature data for both serving and training purposes based on feature access patterns
* __Access__ features for training and serving
* __Discover__ information about entities and features stored and served by Feast

## Motivation

__Access to features in serving__: Machine learning models typically require access to features created in both batch pipelines, and real time streams. Feast provides a means for accessing these features in a serving environment, at low latency and high load.

__Consistency between training and serving__: In many machine learning systems there exists a disconnect between features that are created in batch pipelines for the training of a model, and ones that are created from streams for the serving of real-time features. By centralizing the ingestion of features, Feast provides a consistent view of both batch and real-time features, in both training and serving.

__Infrastructure management__: Feast abstracts away much of the engineering overhead associated with managing data infrastructure. It handles the ingestion, storage, and serving of large amount of feature data in a scalable way. The system  configures data models based on your registered feature specifications, and ensures that you always have a consistent view of features in both your historical and real-time data stores.

__Feature standardisation__: Feast presents a centralized platform on which teams can register features in a standardized way using specifications. This provides structure to the way features are defined and allows teams to reference features in discussions with a singly understood link. 

__Discovery__: Feast allows users to easily explore and discover features and their associated information. This allows for a deeper understanding of features and theirs specifications, more feature reuse between teams and projects, and faster experimentation. Each new ML project can leverage features that have been created by prior teams, which compounds an organization's ability to discover new insights. 

## More Information

* [Components](docs/components.md)
* [Concepts](docs/concepts.md)

For Feast administrators:
* [Installation quickstart](docs/install.md)
* [Helm charts](charts/README.md) details

## Notice

Feast is still under active development. Your feedback and contributions are important to us. Please check our [contributing guide](CONTRIBUTING.md) for details.

## Source Code Headers

Every file containing source code must include copyright and license
information. This includes any JS/CSS files that you might be serving out to
browsers. (This is to help well-intentioned people avoid accidental copying that
doesn't comply with the license.)

Apache header:

    Copyright 2018 The Feast Authors

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
