import argparse
import json
import os
import tempfile
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request
from urllib.request import urlopen

import pandas as pd

from feast import Entity, FeatureService, FeatureStore, FeatureView, Field, RepoConfig
from feast.infra.offline_stores.contrib.chronon_offline_store.chronon_source import (
    ChrononSource,
)
from feast.repo_config import RegistryConfig
from feast.types import Float32, Int64
from feast.value_type import ValueType

QUICKSTART_JOIN = "quickstart/training_set.v2"
PURCHASE_SUM_30D = "quickstart_purchases_v1_purchase_price_sum_30d"
PURCHASE_SUM_14D = "quickstart_purchases_v1_purchase_price_sum_14d"
PURCHASE_COUNT_30D = "quickstart_purchases_v1_purchase_price_count_30d"
PURCHASE_COUNT_14D = "quickstart_purchases_v1_purchase_price_count_14d"
PURCHASE_AVERAGE_30D = "quickstart_purchases_v1_purchase_price_average_30d"
REFUND_SUM_30D = "quickstart_returns_v1_refund_amt_sum_30d"
REFUND_SUM_14D = "quickstart_returns_v1_refund_amt_sum_14d"
REFUND_COUNT_30D = "quickstart_returns_v1_refund_amt_count_30d"
REFUND_COUNT_14D = "quickstart_returns_v1_refund_amt_count_14d"
REFUND_AVERAGE_30D = "quickstart_returns_v1_refund_amt_average_30d"
CHECKOUT_RISK_FEATURE_VIEW = "checkout_risk_features"
CHECKOUT_RISK_FEATURE_SERVICE = "checkout_risk_v1"
CHECKOUT_RISK_DEFAULT_USER_IDS = ["5", "7", "999999"]
CHECKOUT_RISK_FEATURES = [
    PURCHASE_SUM_14D,
    PURCHASE_SUM_30D,
    PURCHASE_COUNT_14D,
    PURCHASE_COUNT_30D,
    PURCHASE_AVERAGE_30D,
    REFUND_SUM_14D,
    REFUND_SUM_30D,
    REFUND_COUNT_14D,
    REFUND_COUNT_30D,
    REFUND_AVERAGE_30D,
]


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


def create_checkout_risk_feature_store(
    repo_path: Path, service_url: str
) -> tuple[FeatureStore, FeatureService, list[str]]:
    repo_path.mkdir(parents=True, exist_ok=True)
    placeholder_path = repo_path / "checkout_risk_placeholder.parquet"
    pd.DataFrame(
        {
            "event_timestamp": [pd.Timestamp("2024-01-01T00:00:00Z")],
        }
    ).to_parquet(placeholder_path)

    store = FeatureStore(
        config=_repo_config(
            repo_path=repo_path,
            project="chronon_checkout_risk",
            offline_store={"type": "chronon"},
            online_store={"type": "chronon", "path": service_url},
        )
    )
    user = Entity(
        name="user",
        join_keys=["user_id"],
        value_type=ValueType.STRING,
    )
    checkout_risk_features = FeatureView(
        name=CHECKOUT_RISK_FEATURE_VIEW,
        entities=[user],
        schema=[
            Field(name=PURCHASE_SUM_14D, dtype=Int64),
            Field(name=PURCHASE_SUM_30D, dtype=Int64),
            Field(name=PURCHASE_COUNT_14D, dtype=Int64),
            Field(name=PURCHASE_COUNT_30D, dtype=Int64),
            Field(name=PURCHASE_AVERAGE_30D, dtype=Float32),
            Field(name=REFUND_SUM_14D, dtype=Int64),
            Field(name=REFUND_SUM_30D, dtype=Int64),
            Field(name=REFUND_COUNT_14D, dtype=Int64),
            Field(name=REFUND_COUNT_30D, dtype=Int64),
            Field(name=REFUND_AVERAGE_30D, dtype=Float32),
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
    feature_service = FeatureService(
        name=CHECKOUT_RISK_FEATURE_SERVICE,
        features=[checkout_risk_features],
        description="Checkout-time risk features served by Chronon.",
        tags={"demo": "chronon", "scenario": "checkout-risk"},
    )
    store.apply([user, checkout_risk_features, feature_service])
    feature_refs = [
        f"{CHECKOUT_RISK_FEATURE_VIEW}:{feature_name}"
        for feature_name in CHECKOUT_RISK_FEATURES
    ]
    return store, feature_service, feature_refs


def fetch_direct_chronon_join(
    service_url: str, user_ids: list[str]
) -> dict[str, object]:
    url = (
        f"{service_url.rstrip('/')}/v1/features/join/{quote(QUICKSTART_JOIN, safe='')}"
    )
    request = Request(
        url=url,
        data=json.dumps([{"user_id": user_id} for user_id in user_ids]).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def _get_response_value(
    response: dict[str, list[object]], feature_name: str, index: int
) -> object:
    values = response.get(feature_name, [])
    if index >= len(values):
        return None
    return values[index]


def summarize_checkout_risk_response(
    response: dict[str, list[object]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, user_id in enumerate(response.get("user_id", [])):
        purchase_sum = _get_response_value(response, PURCHASE_SUM_30D, index)
        purchase_count = _get_response_value(response, PURCHASE_COUNT_30D, index)
        refund_sum = _get_response_value(response, REFUND_SUM_30D, index)
        refund_count = _get_response_value(response, REFUND_COUNT_30D, index)

        ratio = None
        if isinstance(purchase_sum, (int, float)) and isinstance(
            refund_sum, (int, float)
        ):
            ratio = round(refund_sum / purchase_sum, 3) if purchase_sum else None

        rows.append(
            {
                "user_id": user_id,
                "purchase_sum_30d": purchase_sum,
                "purchase_count_30d": purchase_count,
                "refund_sum_30d": refund_sum,
                "refund_count_30d": refund_count,
                "refund_to_purchase_ratio_30d": ratio,
                "status": (
                    "features_found"
                    if any(
                        value is not None
                        for value in [
                            purchase_sum,
                            purchase_count,
                            refund_sum,
                            refund_count,
                        ]
                    )
                    else "missing_features"
                ),
            }
        )
    return rows


def run_checkout_risk_demo(service_url: str, user_ids: list[str]) -> dict[str, object]:
    _assert_service_available(service_url)
    with tempfile.TemporaryDirectory(prefix="feast-chronon-checkout-risk-") as tmp:
        repo_path = Path(tmp)
        store, feature_service, feature_refs = create_checkout_risk_feature_store(
            repo_path, service_url
        )
        entity_rows = [{"user_id": user_id} for user_id in user_ids]
        direct_response = fetch_direct_chronon_join(service_url, user_ids)
        feast_response = store.get_online_features(
            features=feature_service,
            entity_rows=entity_rows,
        ).to_dict()

        return {
            "registry_objects": {
                "entities": [entity.name for entity in store.list_entities()],
                "feature_views": [
                    feature_view.name for feature_view in store.list_feature_views()
                ],
                "feature_services": [
                    service.name for service in store.list_feature_services()
                ],
            },
            "feature_refs": feature_refs,
            "direct_chronon": direct_response,
            "feast_online": feast_response,
            "checkout_risk_summary": summarize_checkout_risk_response(feast_response),
        }


def _parse_user_ids(user_ids: str) -> list[str]:
    return [user_id.strip() for user_id in user_ids.split(",") if user_id.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Feast Chronon demo.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--offline-only", action="store_true")
    mode.add_argument("--online-only", action="store_true")
    parser.add_argument(
        "--scenario",
        choices=["basic", "checkout-risk"],
        default="basic",
        help="Demo scenario to run.",
    )
    parser.add_argument(
        "--service-url",
        default=os.environ.get("CHRONON_SERVICE_URL", "http://127.0.0.1:9000"),
        help="Chronon service URL for the online demo.",
    )
    parser.add_argument(
        "--user-ids",
        default=",".join(CHECKOUT_RISK_DEFAULT_USER_IDS),
        help="Comma-separated user IDs for the checkout-risk scenario.",
    )
    args = parser.parse_args()

    if not args.online_only:
        offline_result = run_offline_demo()
        print("Offline historical features:")
        print(offline_result.to_string(index=False))

    if not args.offline_only:
        if args.scenario == "checkout-risk":
            checkout_result = run_checkout_risk_demo(
                args.service_url,
                _parse_user_ids(args.user_ids),
            )
            print("\nCheckout risk registry objects:")
            print(json.dumps(checkout_result["registry_objects"], indent=2))
            print("\nCheckout risk feature refs:")
            print(json.dumps(checkout_result["feature_refs"], indent=2))
            print("\nDirect Chronon join response:")
            print(
                json.dumps(checkout_result["direct_chronon"], indent=2, sort_keys=True)
            )
            print("\nFeast FeatureService online response:")
            print(json.dumps(checkout_result["feast_online"], indent=2, sort_keys=True))
            print("\nCheckout risk summary:")
            print(
                pd.DataFrame(checkout_result["checkout_risk_summary"]).to_string(
                    index=False,
                    na_rep="null",
                )
            )
        else:
            online_result = run_online_demo(args.service_url)
            print("\nOnline quickstart features:")
            print(json.dumps(online_result, indent=2, sort_keys=True))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
