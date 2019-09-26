# Contributing Guide

## Code reviews

Code submission to Feast (including submission from project maintainers) requires review and approval.
Please submit a **pull request** to initiate the code review process. We use [prow](https://github.com/kubernetes/test-infra/tree/master/prow) to manage the testing and reviewing of pull requests. Please refer to [config.yaml](../.prow/config.yaml) for details on the test jobs. 

## Code conventions

### Java

We conform to the [java google style guide](https://google.github.io/styleguide/javaguide.html)

If using intellij please import the code styles:
https://github.com/google/styleguide/blob/gh-pages/intellij-java-google-style.xml

### Go

Make sure you apply `go fmt`.