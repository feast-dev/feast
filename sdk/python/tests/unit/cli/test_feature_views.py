from datetime import timedelta
from unittest.mock import Mock, patch

import yaml
from click.testing import CliRunner

from feast import FeatureView, OnlineConfig
from feast.cli.feature_views import feature_views_cmd


def _describe(feature_view: FeatureView) -> dict:
    store = Mock()
    store.get_feature_view.return_value = feature_view

    with patch("feast.cli.feature_views.create_feature_store", return_value=store):
        result = CliRunner().invoke(feature_views_cmd, ["describe", feature_view.name])

    assert result.exit_code == 0, result.output
    return yaml.safe_load(result.output)


def test_feature_view_describe_shows_sequence_online_config() -> None:
    data = _describe(
        FeatureView(
            name="events",
            online_config=OnlineConfig(
                mode="sequence",
                max_length=50,
                max_age=timedelta(days=90),
                write_mode="append",
            ),
        )
    )

    assert data["spec"]["onlineConfig"] == {
        "mode": "sequence",
        "maxLength": 50,
        "maxAgeSeconds": 90 * 24 * 60 * 60,
        "writeMode": "append",
    }


def test_feature_view_describe_shows_explicit_online_config_defaults() -> None:
    data = _describe(FeatureView(name="events", online_config=OnlineConfig()))

    assert data["spec"]["onlineConfig"] == {
        "mode": "latest",
        "maxLength": None,
        "maxAgeSeconds": None,
        "writeMode": "overwrite",
    }


def test_feature_view_describe_omits_absent_online_config() -> None:
    data = _describe(FeatureView(name="events"))

    assert "onlineConfig" not in data["spec"]
