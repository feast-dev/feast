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
        (
            {"mode": "sequence", "max_length": 0, "write_mode": "append"},
            "max_length must be a positive integer",
        ),
        ({"max_age": timedelta(0)}, "max_age must be a positive timedelta"),
        (
            {"mode": "sequence", "max_length": 10},
            "mode='sequence' requires write_mode='append'",
        ),
        (
            {"mode": "sequence", "write_mode": "append"},
            "mode='sequence' requires max_length to be set",
        ),
    ],
)
def test_online_config_validation(kwargs: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        OnlineConfig(**kwargs)
