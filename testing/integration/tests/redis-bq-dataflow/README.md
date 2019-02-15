# redis-bq-dataflow

This repository contains an end-to-end test that sets up the following infrastructure:

1. Feast configured to run jobs on dataflow
2. Redis as serving store
3. BQ as warehouse store

And then runs the tests in `test_feast.py`.