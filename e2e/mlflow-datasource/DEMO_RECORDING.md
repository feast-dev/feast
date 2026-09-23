# MLflow DataSource Demo Recording Guide

Record on your RHOAI cluster (~12 minutes). Use this checklist while running `run_all.sh` or individual scripts.

## Prerequisites

```bash
oc login <cluster>
oc project <namespace>
export MLFLOW_TRACKING_URI="$(oc get mlflow -o jsonpath='{.items[0].status.address.url}')"
export MLFLOW_TRACKING_TOKEN="$(oc whoami -t)"
export REQUESTS_CA_BUNDLE="${REQUESTS_CA_BUNDLE:-/etc/pki/tls/certs/ca-bundle.crt}"

cd e2e/mlflow-datasource
pip install -e ../../sdk/python 'feast[mlflow]'
```

## Acts

| Act | Show |
|-----|------|
| 1. Context | Branch, `oc whoami`, MLflow UI |
| 2. Seed | `./run_all.sh` step 0 or `python3 seed_mlflow.py` + artifacts in UI |
| 3. Apply | `feast apply`, `feast mlflow list-sources`, `feast mlflow validate-source mlflow_parquet_features` |
| 4. Retrieve | `python3 test_happy_path.py` output (DataFrames, saved dataset) |
| 5. Errors | `python3 test_error_handling.py`; optional scale MLflow to 0 and re-run file baseline |
| 6. Auth | `python3 test_auth.py` with good vs bad token |
| 7. Summary | Dual-mode, auth-safe, graceful degradation |

## Key messages

- Feast reads MLflow via the Tracking API (no manual ETL).
- User tokens propagate; no auth bypass.
- Non-MLflow FeatureViews keep working when MLflow is down.
