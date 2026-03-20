import pytest

from feast.utils import _parse_feature_ref, _strip_version_from_ref
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

    def test_feature_view_proto_roundtrip_latest_zero(self):
        """version='latest' with current_version_number=None is preserved as
        None through proto roundtrip (proto default 0 without spec.version
        is treated as None for backward compatibility)."""
        from datetime import timedelta

        from feast.entity import Entity
        from feast.feature_view import FeatureView

        entity = Entity(name="entity_id", join_keys=["entity_id"])
        fv = FeatureView(
            name="test_fv",
            entities=[entity],
            ttl=timedelta(days=1),
            # default version="latest", current_version_number=None
        )
        assert fv.current_version_number is None

        proto = fv.to_proto()
        fv2 = FeatureView.from_proto(proto)
        assert fv2.version == "latest"
        # Proto default 0 without spec.version is treated as None
        assert fv2.current_version_number is None

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


class TestParseFeatureRef:
    def test_bare_ref(self):
        fv, version, feat = _parse_feature_ref("driver_stats:trips")
        assert fv == "driver_stats"
        assert version is None
        assert feat == "trips"

    def test_versioned_ref(self):
        fv, version, feat = _parse_feature_ref("driver_stats@v2:trips")
        assert fv == "driver_stats"
        assert version == 2
        assert feat == "trips"

    def test_latest_ref(self):
        fv, version, feat = _parse_feature_ref("driver_stats@latest:trips")
        assert fv == "driver_stats"
        assert version is None
        assert feat == "trips"

    def test_v0_ref(self):
        fv, version, feat = _parse_feature_ref("driver_stats@v0:trips")
        assert fv == "driver_stats"
        assert version == 0
        assert feat == "trips"

    def test_uppercase_v(self):
        fv, version, feat = _parse_feature_ref("driver_stats@V3:trips")
        assert fv == "driver_stats"
        assert version == 3
        assert feat == "trips"

    def test_invalid_no_colon(self):
        with pytest.raises(ValueError, match="Invalid feature reference"):
            _parse_feature_ref("driver_stats_trips")

    def test_invalid_version_format(self):
        with pytest.raises(ValueError, match="Invalid version"):
            _parse_feature_ref("driver_stats@abc:trips")

    def test_empty_version(self):
        fv, version, feat = _parse_feature_ref("driver_stats@:trips")
        assert fv == "driver_stats"
        assert version is None
        assert feat == "trips"


class TestStripVersionFromRef:
    def test_bare_ref(self):
        assert _strip_version_from_ref("driver_stats:trips") == "driver_stats:trips"

    def test_versioned_ref(self):
        assert _strip_version_from_ref("driver_stats@v2:trips") == "driver_stats:trips"

    def test_latest_ref(self):
        assert (
            _strip_version_from_ref("driver_stats@latest:trips") == "driver_stats:trips"
        )


class TestTableId:
    def test_no_version(self):
        from datetime import timedelta

        from feast.entity import Entity
        from feast.feature_view import FeatureView
        from feast.infra.online_stores.sqlite import _table_id

        entity = Entity(name="entity_id", join_keys=["entity_id"])
        fv = FeatureView(
            name="test_fv",
            entities=[entity],
            ttl=timedelta(days=1),
        )
        assert _table_id("my_project", fv) == "my_project_test_fv"

    def test_v0_no_suffix(self):
        from datetime import timedelta

        from feast.entity import Entity
        from feast.feature_view import FeatureView
        from feast.infra.online_stores.sqlite import _table_id

        entity = Entity(name="entity_id", join_keys=["entity_id"])
        fv = FeatureView(
            name="test_fv",
            entities=[entity],
            ttl=timedelta(days=1),
        )
        fv.current_version_number = 0
        assert _table_id("my_project", fv) == "my_project_test_fv"

    def test_v1_with_suffix(self):
        from datetime import timedelta

        from feast.entity import Entity
        from feast.feature_view import FeatureView
        from feast.infra.online_stores.sqlite import _table_id

        entity = Entity(name="entity_id", join_keys=["entity_id"])
        fv = FeatureView(
            name="test_fv",
            entities=[entity],
            ttl=timedelta(days=1),
        )
        fv.current_version_number = 1
        assert (
            _table_id("my_project", fv, enable_versioning=True)
            == "my_project_test_fv_v1"
        )

    def test_v5_with_suffix(self):
        from datetime import timedelta

        from feast.entity import Entity
        from feast.feature_view import FeatureView
        from feast.infra.online_stores.sqlite import _table_id

        entity = Entity(name="entity_id", join_keys=["entity_id"])
        fv = FeatureView(
            name="test_fv",
            entities=[entity],
            ttl=timedelta(days=1),
        )
        fv.current_version_number = 5
        assert (
            _table_id("my_project", fv, enable_versioning=True)
            == "my_project_test_fv_v5"
        )


class TestValidateFeatureRefsVersioned:
    def test_versioned_refs_no_collision_with_full_names(self):
        from feast.utils import _validate_feature_refs

        # Different versions of the same feature should not collide with full names
        refs = ["driver_stats@v1:trips", "driver_stats@v2:trips"]
        _validate_feature_refs(refs, full_feature_names=True)  # Should not raise

    def test_versioned_refs_collision_without_full_names(self):
        from feast.errors import FeatureNameCollisionError
        from feast.utils import _validate_feature_refs

        # Same feature name from different versions collides without full names
        refs = ["driver_stats@v1:trips", "driver_stats@v2:trips"]
        with pytest.raises(FeatureNameCollisionError):
            _validate_feature_refs(refs, full_feature_names=False)


def _dummy_transformation():
    from feast.transformation.python_transformation import PythonTransformation

    def identity(features_df):
        return features_df

    return PythonTransformation(
        udf=identity,
        udf_string="def identity(features_df):\n    return features_df\n",
    )
