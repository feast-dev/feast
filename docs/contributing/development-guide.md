# Development Guide

## Overview

The following guide will help you quickly run Feast in your local machine.

The main components of Feast are:

* **Feast Core:** Handles feature registration and ensures that Feast internal metadata is consistent.
* **Feast Serving:** Service that handles requests for features values.

## Requirements

### **Development environment**

The following software is required for Feast development

* Java SE Development Kit 11
* Python version 3.6 \(or above\) and pip
* [Maven](https://maven.apache.org/install.html) version 3.6.x
* PySpark 2.4.2

### **Services**

The following components/services are required to develop Feast:

* **PostgreSQL \(version 11 and above\)**: Required by Feast Core
* **Redis \(tested on version 5.x\)**: Required by Feast Serving

These services should be running before starting development. The following snippet will start the services using Docker:

```bash
# Start Postgres
docker run --name postgres --rm -it -d -e POSTGRES_DB=postgres -e POSTGRES_USER=postgres \
-e POSTGRES_PASSWORD=password -p 5432:5432 postgres:12-alpine

# Start Redis
docker run --name redis --rm -it -d -p 6379:6379 redis:5-alpine
```

## Setting up

#### Assumptions

* PostgreSQL is running in `localhost:5432` and has a database called `postgres` which

  can be accessed with credentials user `postgres` and password `password`. Different database configurations can be supplied here via Feast Core's `application.yml`.

* Redis is running locally and accessible from `localhost:6379`
* \(optional\) The local environment has been authentication with Google Cloud Platform and has full access to BigQuery. This is only necessary for BigQuery testing/development.

Clone Feast repository.

```bash
git clone https://github.com/feast-dev/feast.git && cd feast
```

### How to compile Feast Core & Serving jar files?

```bash
# Compile and package Feast components
mvn package -Dmaven.test.skip=true
```

### How to configure Feast Core & Serving?

Feast Core is configured through [Feast Core's application.yml](https://github.com/feast-dev/feast/blob/master/core/src/main/resources/application.yml).

Feast Serving is configured through [Feast Serving's application.yml](https://github.com/feast-dev/feast/blob/master/serving/src/main/resources/application.yml). Each Serving deployment must be configured with a store. Serving uses the store specified by `active-store` in `application.yml`

```text
# Indicates the active store. Only a single store in the last can be active at one time.
active_store: online
```

The default store `online` configured is Redis \(used for online serving\):

```text
  stores:
    - name: online # Name of the store (referenced by active_store)
      type: REDIS 
      config:  # store specific config. In this case specifies the host:port of redis.
        host: localhost
        port: 6379
     # ......
```

### How to configure SDK development environment?

In [Feast repository](https://github.com/feast-dev/feast) directory, you will be able to find a [Makefile](https://github.com/feast-dev/feast/blob/master/Makefile).

#### Python

You are encouraged to create and use a virtual environment. Setup your python development environment with a simple command:

```bash
make install-python
```

#### Go

```bash
make compile-protos-go
```

### How to start Feast Core & Serving?

Feast Serving has a dependency on Feast Core, thus always start Feast Core first.

```bash
# Start Feast Core locally
java -jar core/target/feast-core-0.9.2-SNAPSHOT-exec.jar

# Start Feast Serving locally
java -jar serving/target/feast-serving-0.9.2-SNAPSHOT-exec.jar
```

Test whether Feast Core, Feast Serving are started and running correctly:

```bash
feast version --core-url="localhost:6565" --serving-url="localhost:6566"
```

```javascript
{
    'serving': {'url': 'localhost:6566', 'version': '0.9.2-SNAPSHOT'},
    'core': {'url': 'localhost:6565', 'version': '0.9.2-SNAPSHOT'}
}
```

## Running tests

#### Java

```bash
# Unit tests
mvn test

# Integration tests
mvn verify
```

#### Python

```bash
# Unit tests
make test-python
```

#### Go

```bash
# Unit tests
make test-go
```

## Style Guide

### Language Specific Style Guides

#### Java

We conform to the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html). Maven can helpfully take care of that for you before you commit:

```text
$ mvn spotless:apply
```

Formatting will be checked automatically during the `verify` phase. This can be skipped temporarily:

```text
$ mvn spotless:check  # Check is automatic upon `mvn verify`
$ mvn verify -Dspotless.check.skip
```

If you're using IntelliJ, you can import [these code style settings](https://github.com/google/styleguide/blob/gh-pages/intellij-java-google-style.xml) if you'd like to use the IDE's reformat function as you develop.

#### Go

Make sure you apply `go fmt`.

#### Python

We use [Python Black](https://github.com/psf/black) to format our Python code prior to submission.

### Formatting and Linting

Code can automatically be formatted by running the following command from the project root directory

```text
make format
```

Once code that is submitted through a PR or direct push will be validated with the following command

```text
make lint
```

## Making a PR

#### Incorporating upstream changes from master

Our preference is the use of `git rebase` instead of `git merge`.

#### Signing commits

```text
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

