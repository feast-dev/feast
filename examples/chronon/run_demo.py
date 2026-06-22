import argparse
import json
import os
import tempfile
from pathlib import Path
from urllib.request import urlopen

import pandas as pd

from feast import Entity, FeatureStore, FeatureView, Field, RepoConfig
from feast.infra.offline_stores.contrib.chronon_offline_store.chronon_source import (
    ChrononSource,
)
from feast.repo_config import RegistryConfig
from feast.types import Float32, Int64
from feast.value_type import ValueType

QUICKSTART_JOIN = "quickstart/training_set.v2"
PURCHASE_SUM_30D = "quickstart_purchases_v1_purchase_price_sum_30d"
REFUND_SUM_30D = "quickstart_returns_v1_refund_amt_sum_30d"


def _repo_config(
    repo_path: Path,
    project: str,
    offline_store: dict[str, str],
    online_store: dict[str, str],
) -> RepoConfig:
    config = RepoConfig(
        project=project,
        registry=RegistryConfig(path=str(repo_path / "registry.db")),
        provider="chronon",
        offline_store=offline_store,
        online_store=online_store,
    )
    config.repo_path = repo_path
    return config


def run_offline_demo() -> pd.DataFrame:
    with tempfile.TemporaryDirectory(prefix="feast-chronon-offline-") as tmp:
        repo_path = Path(tmp)
        materialization_path = repo_path / "driver_stats.parquet"
        pd.DataFrame(
            {
                "driver_id": [1001, 1001, 1002],
                "event_timestamp": [
                    pd.Timestamp("2024-01-01T00:00:00Z"),
                    pd.Timestamp("2024-01-03T00:00:00Z"),
                    pd.Timestamp("2024-01-02T00:00:00Z"),
                ],
                "completed_rides_7d": [12, 18, 7],
                "average_rating": [4.6, 4.8, 4.2],
            }
        ).to_parquet(materialization_path)

        store = FeatureStore(
            config=_repo_config(
                repo_path=repo_path,
                project="chronon_demo_offline",
                offline_store={"type": "chronon"},
                online_store={"type": "sqlite", "path": str(repo_path / "online.db")},
            )
        )
        driver = Entity(
            name="driver",
            join_keys=["driver_id"],
            value_type=ValueType.INT64,
        )
        driver_stats = FeatureView(
            name="driver_stats",
            entities=[driver],
            schema=[
                Field(name="completed_rides_7d", dtype=Int64),
                Field(name="average_rating", dtype=Float32),
            ],
            source=ChrononSource(
                materialization_path=str(materialization_path),
                chronon_join="demo/driver_stats.v1",
                timestamp_field="event_timestamp",
            ),
            offline=True,
        )
        store.apply([driver, driver_stats])

        entity_df = pd.DataFrame(
            {
                "driver_id": [1001, 1002],
                "event_timestamp": [
                    pd.Timestamp("2024-01-04T00:00:00Z"),
                    pd.Timestamp("2024-01-02T12:00:00Z"),
                ],
            }
        )
        result = store.get_historical_features(
            entity_df=entity_df,
            features=[
                "driver_stats:completed_rides_7d",
                "driver_stats:average_rating",
            ],
        ).to_df()

    assert result["completed_rides_7d"].tolist() == [18, 7]
    assert result["average_rating"].round(1).tolist() == [4.8, 4.2]
    return result


def _assert_service_available(service_url: str) -> None:
    ping_url = f"{service_url.rstrip('/')}/ping"
    try:
        with urlopen(ping_url, timeout=5) as response:
            response.read()
    except OSError as exc:
        raise RuntimeError(
            f"Chronon service is not reachable at {ping_url}. Start it with "
            "`infra/scripts/chronon/start-local-chronon-service.sh` or pass "
            "`--offline-only`."
        ) from exc


def run_online_demo(service_url: str) -> dict[str, list[object]]:
    _assert_service_available(service_url)
    with tempfile.TemporaryDirectory(prefix="feast-chronon-online-") as tmp:
        repo_path = Path(tmp)
        placeholder_path = repo_path / "placeholder.parquet"
        pd.DataFrame(
            {
                "event_timestamp": [pd.Timestamp("2024-01-01T00:00:00Z")],
            }
        ).to_parquet(placeholder_path)

        store = FeatureStore(
            config=_repo_config(
                repo_path=repo_path,
                project="chronon_demo_online",
                offline_store={"type": "chronon"},
                online_store={"type": "chronon", "path": service_url},
            )
        )
        user = Entity(
            name="user",
            join_keys=["user_id"],
            value_type=ValueType.STRING,
        )
        training_set = FeatureView(
            name="training_set",
            entities=[user],
            schema=[
                Field(name=PURCHASE_SUM_30D, dtype=Int64),
                Field(name=REFUND_SUM_30D, dtype=Int64),
            ],
            source=ChrononSource(
                materialization_path=str(placeholder_path),
                chronon_join=QUICKSTART_JOIN,
                timestamp_field="event_timestamp",
                online_endpoint=service_url,
            ),
            online=True,
            offline=False,
        )
        store.apply([user, training_set])

        response = store.get_online_features(
            features=[
                f"training_set:{PURCHASE_SUM_30D}",
                f"training_set:{REFUND_SUM_30D}",
            ],
            entity_rows=[{"user_id": "5"}],
        ).to_dict()

    assert response[PURCHASE_SUM_30D] == [1253]
    assert response[REFUND_SUM_30D] == [1269]
    return response


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Feast Chronon demo.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--offline-only", action="store_true")
    mode.add_argument("--online-only", action="store_true")
    parser.add_argument(
        "--service-url",
        default=os.environ.get("CHRONON_SERVICE_URL", "http://127.0.0.1:9000"),
        help="Chronon service URL for the online demo.",
    )
    args = parser.parse_args()

    if not args.online_only:
        offline_result = run_offline_demo()
        print("Offline historical features:")
        print(offline_result.to_string(index=False))

    if not args.offline_only:
        online_result = run_online_demo(args.service_url)
        print("\nOnline quickstart features:")
        print(json.dumps(online_result, indent=2, sort_keys=True))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
