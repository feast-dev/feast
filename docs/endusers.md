# Feast End Users Quickstart Guide

## Pre-requisities

* A working Feast Core: Consult your Feast admin or [install your own](install.md).
* Feast CLI tools: Use [pre-built
  binaries](https://github.com/gojek/feast/releases) or [compile your
  own](../cli/README.md).

Make sure your CLI is correctly configured for your Feast Core. If
you're running a local Fesst Core, it would be:
```sh
feast config set coreURI localhost
```

## Introduction

There are several stages to using Feast:
1. Register your feature
2. Ingest data for your feature
3. Query feature data for training your models
4. Query feature data for serving your models

## Registering your feature

In order to register a feature, you will first need to register a:
* Storage location (typically done by your Feast admin)
* Entity

All registrations are done using [specs](specs.md).

### Registering an entity

Then register an entity, which is for grouping features under a unique
key or id. Typically these map to a domain object, e.g., a customer, a
merchant, a sales region.

`wordEntity.yml`
```
name: word
description: word found in shakespearean works
```

Register the entity spec:
```sh
feast apply entity wordEntity.yml
```

### Registering your feature

Next, define your feature:

`wordCount.yml`
```
id: word.none.count
name: count
entity: word
owner: bob@feast.com
description: number of times the word appears
valueType:  INT64
granularity: NONE
uri: https://github.com/bob/example
dataStores:
  serving:
    id: REDIS1
  warehouse:
    id: BIGQUERY1
```

Register it:
```sh
feast apply feature wordCount.yml
```
