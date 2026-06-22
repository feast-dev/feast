import importlib.util
from pathlib import Path


def _load_chronon_demo():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = repo_root / "examples" / "chronon" / "run_demo.py"
    spec = importlib.util.spec_from_file_location("chronon_run_demo", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_checkout_risk_feature_repo_registers_expected_objects(tmp_path):
    demo = _load_chronon_demo()

    store, feature_service, feature_refs = demo.create_checkout_risk_feature_store(
        tmp_path, "http://chronon.test"
    )

    assert feature_service.name == "checkout_risk_v1"
    assert feature_refs == [
        f"checkout_risk_features:{feature_name}"
        for feature_name in demo.CHECKOUT_RISK_FEATURES
    ]
    assert [entity.name for entity in store.list_entities()] == ["user"]
    assert [view.name for view in store.list_feature_views()] == [
        "checkout_risk_features"
    ]
    assert [service.name for service in store.list_feature_services()] == [
        "checkout_risk_v1"
    ]


def test_checkout_risk_summary_marks_missing_users():
    demo = _load_chronon_demo()

    rows = demo.summarize_checkout_risk_response(
        {
            "user_id": ["5", "999999"],
            demo.PURCHASE_SUM_30D: [1253, None],
            demo.PURCHASE_COUNT_30D: [5, None],
            demo.REFUND_SUM_30D: [1269, None],
            demo.REFUND_COUNT_30D: [5, None],
        }
    )

    assert rows == [
        {
            "user_id": "5",
            "purchase_sum_30d": 1253,
            "purchase_count_30d": 5,
            "refund_sum_30d": 1269,
            "refund_count_30d": 5,
            "refund_to_purchase_ratio_30d": 1.013,
            "status": "features_found",
        },
        {
            "user_id": "999999",
            "purchase_sum_30d": None,
            "purchase_count_30d": None,
            "refund_sum_30d": None,
            "refund_count_30d": None,
            "refund_to_purchase_ratio_30d": None,
            "status": "missing_features",
        },
    ]
