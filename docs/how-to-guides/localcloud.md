# LocalCloud integration

<!-- Generated from the LocalCloud integration run's final report. -->

This project's Google Cloud integrations can run against [LocalCloud](https://local.cloud), a local Google Cloud emulator, without a Google Cloud project or credentials.

## Services on LocalCloud

| Service | Used by | Env var | Default endpoint | Wiring |
|---|---|---|---|---|
| [Cloud Storage](https://local.cloud/cloud-storage-emulator/) | Python GCS registry | `STORAGE_EMULATOR_HOST` | `http://localhost:5382` | Env var, no code change |
| [Bigtable](https://local.cloud/bigtable-emulator/) | Python Bigtable online store | `BIGTABLE_EMULATOR_HOST` | `localhost:5385` | Env var, no code change |
| [BigQuery](https://local.cloud/bigquery-emulator/) | Python BigQuery offline store | `BIGQUERY_EMULATOR_HOST` | `http://localhost:5388` | Code change |

`GOOGLE_CLOUD_PROJECT` selects the LocalCloud project. Endpoints are LocalCloud's defaults; the gateway's `GET /env` lists the current ones.

## How it works

Each client reads its LocalCloud setting when it is created: with the setting present it talks to LocalCloud on localhost; without it, nothing changes.

- **Cloud Storage**, Python GCS registry (`sdk/python/feast/infra/registry/gcs.py`): google-cloud-storage reads STORAGE_EMULATOR_HOST, so Feast’s GCS registry needs no client change.
- **Bigtable**, Python Bigtable online store (`sdk/python/feast/infra/online_stores/bigtable.py`): google-cloud-bigtable reads BIGTABLE_EMULATOR_HOST, so Feast’s online store needs no client change.
- **BigQuery**, Python BigQuery offline store (`sdk/python/feast/infra/offline_stores/bigquery.py`, `sdk/python/feast/infra/offline_stores/bigquery_source.py`): Feast’s BigQuery factory uses BIGQUERY_EMULATOR_HOST as its explicit API endpoint in the local lane.

## Run the LocalCloud tests

Install Docker (with at least 4 GB available), Python 3.11+, and the project's build tools. Install `uv` and `make` for the recorded Python build. From the repository root, run:

```bash
python3 scripts/localcloud-test.py
# Also run the documented regression scope:
python3 scripts/localcloud-test.py --full-suite
```

The launcher uses the pinned public image in `.localcloud/tests.json`, starts only the services above, maps their ports automatically, and creates a unique project when `GOOGLE_CLOUD_PROJECT` is unset. Each run owns a separate container and volume; both are removed even after test failures. No Google Cloud credentials or separate service emulators are used.

Build and test commands are recorded in `.localcloud/tests.json`. Use `--skip-build` after dependencies are installed, or `--existing http://127.0.0.1:5380` to borrow a LocalCloud instance with the same enabled services. A supplied project on a borrowed instance is retained; an automatically created project is removed.

## GitHub Actions (optional)

`.github/workflows/localcloud.yml` runs the same launcher on Ubuntu. Once merged onto the default branch, run **LocalCloud tests** manually from the Actions tab. To run it on pull requests, create repository Actions variable `LOCALCLOUD_CI_ENABLED` with value `true`. The job is skipped on pull requests until enabled; existing workflows remain in place.

Regression scope: Python unit regression suite excluding the unrelated MongoDB offline-store module, which contains external MongoDB Testcontainers tests requiring Docker socket access. This does not qualify every Feast CI job. Install Java 21 for Spark tests; uv run selects the project Python for Spark workers. Existing broader CI remains unchanged.

## Outside this test lane

- **Compute Engine**: Unsupported in the compatibility snapshot.
- **Dataproc**: Feast uses Docker-backed cluster deployment and Spark execution; that workload was not qualified in this focused Python lane.
- **datastore**: No complete Datastore service guide was supplied for this run.
- **gcloud**: The inventory identifies a provisioning and publishing CLI, not an emulated application service.
- **GKE**: Non-callable in the compatibility snapshot.
- **google**: The inventory entry denotes the Terraform provider, not a separate service.
- **Memorystore (Redis/Valkey)**: LocalCloud assigns a shared Valkey logical database per instance; Feast’s Terraform-generated Redis configuration and cluster tests were not adapted to that allocation.
- **project**: The inventory entry denotes Terraform project resources; the assigned run project was used without provisioning another.
- **service**: The inventory entry denotes Terraform service-account and IAM resources, not a separate application data service.

## LocalCloud docs

[LocalCloud docs](https://local.cloud/docs/) · [Configuration](https://local.cloud/docs/configuration/) · [SDK examples](https://local.cloud/docs/sdk-examples/) · [Supported services](https://local.cloud/services/)
