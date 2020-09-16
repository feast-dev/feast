# Style Guide

## 1. Language Specific Style Guides

### 1.1 Java

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

### 1.2 Go

Make sure you apply `go fmt`.

### 1.3 Python

We use [Python Black](https://github.com/psf/black) to format our Python code prior to submission.

## 2. Formatting and Linting

Code can automatically be formatted by running the following command from the project root directory

```text
make format
```

Once code that is submitted through a PR or direct push will be validated with the following command

```text
make lint
```

