# Iceberg REST catalog + MinIO smoke test

This example is a deterministic smoke test for the **certified** configuration:

- Iceberg **REST catalog**
- S3-compatible warehouse via **MinIO**

It validates that Feast’s Iceberg offline/online store integrations can:

- connect to a REST Iceberg catalog
- create and append to Iceberg tables in S3-compatible storage
- read data back via the Iceberg online store API (write + read)
- read data back via the Iceberg offline store helper paths (schema resolve + DuckDB read)

## Prerequisites

- Docker + docker compose
- Python with `pyiceberg`, `pyarrow`, and `duckdb` available

From the Feast repo root, run the smoke test using the repo sources:

## Run

```bash
cd examples/iceberg-rest-minio

docker compose up -d

# Run smoke test against the REST catalog
PYTHONPATH=../../sdk/python python smoke_test.py

docker compose down -v
```

## Notes

- The compose stack exposes:
  - MinIO: `http://localhost:9000` (console: `http://localhost:9001`)
  - Iceberg REST: `http://localhost:8181`
- This is intended as a **smoke test**, not a benchmark.
