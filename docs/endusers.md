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

[`wordEntity.yml`](../examples/wordEntity.yml)
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

[`wordCountFeature.yml`](../examples/wordCountFeature.yml)
```
id: word.count
name: count
entity: word
owner: bob@feast.com
description: number of times the word appears
valueType:  INT64
uri: https://github.com/bob/example
dataStores:
  serving:
    id: REDIS1
  warehouse:
    id: BIGQUERY1
```

Register it:
```sh
feast apply feature wordCountFeature.yml
```

## Ingest data for your feature

Feast supports ingesting feature from 4 type of sources:

* File (either CSV or JSON)
* Bigquery Table
* Pubsub Topic
* Pubsub Subscription

[[More details]](specs.md#import-spec)

Let's take a look on how to create an import job spec and ingest some data from a CSV file.

### Prepare your data
`word_counts.csv`
```csv
count,word
28944,the
27317,and
21120,i
20136,to
17181,of
14945,a
13989,you
12949,my
11513,in
11488,that
9545,is
8855,not
8293,with
8043,me
8003,it
...
```  

And then upload it into your Google Storage bucket:

```sh
gsutil cp word_counts.csv gs://your-bucket
```

### Define the job import spec
`shakespeareWordCountsImport.yml`
```yaml
type: file.csv
sourceOptions:
  path: gs://your-bucket/word_counts.csv
entities:
  - word
schema:
  entityIdColumn: word
  timestampValue: 2019-01-01T00:00:00.000Z
  fields:    
    - name: count
      featureId: word.count
    - name: word  
```

### Start the ingestion job
Next, use `feast` CLI to run your ingestion job, defined in 
`shakespeareWordCountsImport.yml`:
```sh
feast jobs run shakespeareWordCountsImport.yml
```

You can also list recent ingestion jobs by running:
```sh
feast list jobs
```

Or get detailed information about the results of ingestion with:
```sh
feast get job <id>
```