from datetime import timedelta

import pytest

from feast import OnlineConfig


def test_online_config_defaults() -> None:
    config = OnlineConfig()

    assert config.mode == "latest"
    assert config.max_length is None
    assert config.max_age is None
    assert config.write_mode == "overwrite"


def test_online_config_proto_round_trip() -> None:
    config = OnlineConfig(
        mode="sequence",
        max_length=50,
        max_age=timedelta(days=90),
        write_mode="append",
    )

    proto = config.to_proto()
    assert proto.mode == proto.SEQUENCE
    assert proto.max_length == 50
    assert proto.max_age_seconds == 90 * 24 * 60 * 60
    assert proto.write_mode == proto.APPEND
    assert OnlineConfig.from_proto(proto) == config


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"mode": "invalid"}, "mode must be"),
        ({"write_mode": "invalid"}, "write_mode must be"),
        ({"max_length": 0}, "max_length must be a positive integer"),
        ({"max_length": -1}, "max_length must be a positive integer"),
        ({"max_length": True}, "max_length must be a positive integer"),
        ({"max_length": 1.5}, "max_length must be a positive integer"),
        (
            {"max_length": 2_147_483_648},
            "max_length must fit in a signed 32-bit integer",
        ),
        ({"max_age": timedelta(0)}, "max_age must be a positive timedelta"),
        ({"max_age": timedelta(seconds=-1)}, "max_age must be a positive timedelta"),
        ({"max_age": "1 day"}, "max_age must be a positive timedelta"),
        ({"max_age": timedelta(microseconds=1)}, "max_age must use whole seconds"),
        (
            {"mode": "sequence", "max_length": 10},
            "mode='sequence' requires write_mode='append'",
        ),
        (
            {"mode": "sequence", "write_mode": "append"},
            "mode='sequence' requires max_length to be set",
        ),
        (
            {"write_mode": "append"},
            "mode='latest' requires write_mode='overwrite'",
        ),
        (
            {"max_length": 10},
            "retention limits are only valid when mode='sequence'",
        ),
        (
            {"max_age": timedelta(days=1)},
            "retention limits are only valid when mode='sequence'",
        ),
    ],
)
def test_online_config_validation(kwargs: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        OnlineConfig(**kwargs)


def test_feature_view_revalidates_mutated_online_config() -> None:
    from feast import FeatureView

    config = OnlineConfig()
    feature_view = FeatureView(name="events", online_config=config)
    config.write_mode = "append"

    with pytest.raises(
        ValueError, match="mode='latest' requires write_mode='overwrite'"
    ):
        feature_view.ensure_valid()
