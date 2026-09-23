# MLflow DataSource E2E (RHOAI)

Real-cluster tests for PR #6702. No mocks for MLflow API calls.

## Prerequisites

- OpenShift login and namespace with Feast + MLflow
- `export MLFLOW_TRACKING_URI=...`
- `export MLFLOW_TRACKING_TOKEN=$(oc whoami -t)` (or SA token)
- `pip install -e ../../sdk/python 'feast[mlflow]'`

## Run

```bash
./run_all.sh
```

See [DEMO_RECORDING.md](DEMO_RECORDING.md) for demo recording steps.
