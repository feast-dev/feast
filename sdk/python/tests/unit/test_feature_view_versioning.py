import pytest

from feast.version_utils import (
    generate_version_id,
    normalize_version_string,
    parse_version,
    version_tag,
)


class TestParseVersion:
    def test_latest_string(self):
        is_latest, num = parse_version("latest")
        assert is_latest is True
        assert num == 0

    def test_empty_string(self):
        is_latest, num = parse_version("")
        assert is_latest is True
        assert num == 0

    def test_latest_case_insensitive(self):
        is_latest, num = parse_version("Latest")
        assert is_latest is True

    def test_v_format(self):
        is_latest, num = parse_version("v2")
        assert is_latest is False
        assert num == 2

    def test_version_format(self):
        is_latest, num = parse_version("version3")
        assert is_latest is False
        assert num == 3

    def test_case_insensitive_v(self):
        is_latest, num = parse_version("V5")
        assert is_latest is False
        assert num == 5

    def test_case_insensitive_version(self):
        is_latest, num = parse_version("Version10")
        assert is_latest is False
        assert num == 10

    def test_v0(self):
        is_latest, num = parse_version("v0")
        assert is_latest is False
        assert num == 0

    def test_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid version string"):
            parse_version("abc")

    def test_invalid_format_no_number(self):
        with pytest.raises(ValueError, match="Invalid version string"):
            parse_version("v")

    def test_invalid_format_negative(self):
        with pytest.raises(ValueError, match="Invalid version string"):
            parse_version("v-1")


class TestNormalizeVersionString:
    def test_empty_to_latest(self):
        assert normalize_version_string("") == "latest"

    def test_latest_unchanged(self):
        assert normalize_version_string("latest") == "latest"

    def test_v2_canonical(self):
        assert normalize_version_string("v2") == "v2"

    def test_version2_to_v2(self):
        assert normalize_version_string("version2") == "v2"

    def test_V3_to_v3(self):
        assert normalize_version_string("V3") == "v3"


class TestVersionTag:
    def test_simple(self):
        assert version_tag(0) == "v0"
        assert version_tag(5) == "v5"
        assert version_tag(100) == "v100"


class TestGenerateVersionId:
    def test_is_uuid(self):
        vid = generate_version_id()
        assert len(vid) == 36
        assert vid.count("-") == 4

    def test_unique(self):
        v1 = generate_version_id()
        v2 = generate_version_id()
        assert v1 != v2


class TestFeatureViewVersionField:
    def test_feature_view_default_version(self):
        from datetime import timedelta

        from feast.entity import Entity
        from feast.feature_view import FeatureView

        entity = Entity(name="entity_id", join_keys=["entity_id"])
        fv = FeatureView(
            name="test_fv",
            entities=[entity],
            ttl=timedelta(days=1),
        )
        assert fv.version == "latest"
        assert fv.current_version_number is None

    def test_feature_view_explicit_version(self):
        from datetime import timedelta

        from feast.entity import Entity
        from feast.feature_view import FeatureView

        entity = Entity(name="entity_id", join_keys=["entity_id"])
        fv = FeatureView(
            name="test_fv",
            entities=[entity],
            ttl=timedelta(days=1),
            version="v2",
        )
        assert fv.version == "v2"

    def test_feature_view_proto_roundtrip(self):
        from datetime import timedelta

        from feast.entity import Entity
        from feast.feature_view import FeatureView

        entity = Entity(name="entity_id", join_keys=["entity_id"])
        fv = FeatureView(
            name="test_fv",
            entities=[entity],
            ttl=timedelta(days=1),
            version="v3",
        )
        fv.current_version_number = 3

        proto = fv.to_proto()
        assert proto.spec.version == "v3"
        assert proto.meta.current_version_number == 3

        fv2 = FeatureView.from_proto(proto)
        assert fv2.version == "v3"
        assert fv2.current_version_number == 3

    def test_feature_view_proto_roundtrip_v0(self):
        from datetime import timedelta

        from feast.entity import Entity
        from feast.feature_view import FeatureView

        entity = Entity(name="entity_id", join_keys=["entity_id"])
        fv = FeatureView(
            name="test_fv",
            entities=[entity],
            ttl=timedelta(days=1),
            version="v0",
        )
        fv.current_version_number = 0

        proto = fv.to_proto()
        assert proto.spec.version == "v0"
        assert proto.meta.current_version_number == 0

        fv2 = FeatureView.from_proto(proto)
        assert fv2.version == "v0"
        assert fv2.current_version_number == 0

    def test_feature_view_equality_with_version(self):
        from datetime import timedelta

        from feast.entity import Entity
        from feast.feature_view import FeatureView

        entity = Entity(name="entity_id", join_keys=["entity_id"])
        fv1 = FeatureView(
            name="test_fv",
            entities=[entity],
            ttl=timedelta(days=1),
            version="v2",
        )
        fv2 = FeatureView(
            name="test_fv",
            entities=[entity],
            ttl=timedelta(days=1),
            version="version2",
        )
        # v2 and version2 should be equivalent
        assert fv1 == fv2

    def test_feature_view_inequality_different_version(self):
        from datetime import timedelta

        from feast.entity import Entity
        from feast.feature_view import FeatureView

        entity = Entity(name="entity_id", join_keys=["entity_id"])
        fv1 = FeatureView(
            name="test_fv",
            entities=[entity],
            ttl=timedelta(days=1),
            version="latest",
        )
        fv2 = FeatureView(
            name="test_fv",
            entities=[entity],
            ttl=timedelta(days=1),
            version="v2",
        )
        assert fv1 != fv2

    def test_feature_view_empty_version_equals_latest(self):
        from datetime import timedelta

        from feast.entity import Entity
        from feast.feature_view import FeatureView

        entity = Entity(name="entity_id", join_keys=["entity_id"])
        fv1 = FeatureView(
            name="test_fv",
            entities=[entity],
            ttl=timedelta(days=1),
        )
        fv2 = FeatureView(
            name="test_fv",
            entities=[entity],
            ttl=timedelta(days=1),
            version="latest",
        )
        assert fv1 == fv2

    def test_feature_view_copy_preserves_version(self):
        import copy
        from datetime import timedelta

        from feast.entity import Entity
        from feast.feature_view import FeatureView

        entity = Entity(name="entity_id", join_keys=["entity_id"])
        fv = FeatureView(
            name="test_fv",
            entities=[entity],
            ttl=timedelta(days=1),
            version="v5",
        )
        fv_copy = copy.copy(fv)
        assert fv_copy.version == "v5"


class TestOnDemandFeatureViewVersionField:
    def test_odfv_default_version(self):
        from feast.data_source import RequestSource
        from feast.field import Field
        from feast.on_demand_feature_view import OnDemandFeatureView
        from feast.types import Float32

        request_source = RequestSource(
            name="vals_to_add",
            schema=[
                Field(name="val_to_add", dtype=Float32),
                Field(name="val_to_add_2", dtype=Float32),
            ],
        )
        odfv = OnDemandFeatureView(
            name="test_odfv",
            sources=[request_source],
            schema=[Field(name="output", dtype=Float32)],
            feature_transformation=_dummy_transformation(),
            mode="python",
        )
        assert odfv.version == "latest"

    def test_odfv_proto_roundtrip(self):
        from feast.data_source import RequestSource
        from feast.field import Field
        from feast.on_demand_feature_view import OnDemandFeatureView
        from feast.types import Float32

        request_source = RequestSource(
            name="vals_to_add",
            schema=[
                Field(name="val_to_add", dtype=Float32),
                Field(name="val_to_add_2", dtype=Float32),
            ],
        )
        odfv = OnDemandFeatureView(
            name="test_odfv",
            sources=[request_source],
            schema=[Field(name="output", dtype=Float32)],
            feature_transformation=_dummy_transformation(),
            mode="python",
            version="v1",
        )
        odfv.current_version_number = 1

        proto = odfv.to_proto()
        assert proto.spec.version == "v1"
        assert proto.meta.current_version_number == 1

        odfv2 = OnDemandFeatureView.from_proto(proto)
        assert odfv2.version == "v1"
        assert odfv2.current_version_number == 1


def _dummy_transformation():
    from feast.transformation.python_transformation import PythonTransformation

    def identity(features_df):
        return features_df

    return PythonTransformation(
        udf=identity,
        udf_string="def identity(features_df):\n    return features_df\n",
    )
