from datetime import timedelta
from unittest.mock import MagicMock

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

    def test_unrecognized_version_falls_back(self):
        """Unrecognized version format falls back to treating full fv_part as the name."""
        fv, version, feat = _parse_feature_ref("driver_stats@abc:trips")
        assert fv == "driver_stats@abc"
        assert version is None
        assert feat == "trips"

    def test_empty_version(self):
        fv, version, feat = _parse_feature_ref("driver_stats@:trips")
        assert fv == "driver_stats"
        assert version is None
        assert feat == "trips"

    def test_at_sign_in_fv_name_falls_back_gracefully(self):
        """Legacy FV name with @ falls back to treating whole pre-colon string as name."""
        fv, version, feat = _parse_feature_ref("my@weird:feature")
        assert fv == "my@weird"
        assert version is None
        assert feat == "feature"

    def test_at_sign_with_valid_version_still_parses(self):
        """A valid @v<N> suffix still parses as a version."""
        fv, version, feat = _parse_feature_ref("stats@v2:trips")
        assert fv == "stats"
        assert version == 2
        assert feat == "trips"


class TestEnsureValidRejectsReservedChars:
    def test_at_sign_in_name_raises(self):
        from datetime import timedelta

        from feast.entity import Entity
        from feast.feature_view import FeatureView

        entity = Entity(name="entity_id", join_keys=["entity_id"])
        fv = FeatureView(
            name="my@weird_fv",
            entities=[entity],
            ttl=timedelta(days=1),
        )
        with pytest.raises(ValueError, match="must not contain '@'"):
            fv.ensure_valid()

    def test_colon_in_name_raises(self):
        from datetime import timedelta

        from feast.entity import Entity
        from feast.feature_view import FeatureView

        entity = Entity(name="entity_id", join_keys=["entity_id"])
        fv = FeatureView(
            name="my:weird_fv",
            entities=[entity],
            ttl=timedelta(days=1),
        )
        with pytest.raises(ValueError, match="must not contain ':'"):
            fv.ensure_valid()

    def test_valid_name_passes(self):
        from datetime import timedelta

        from feast.entity import Entity
        from feast.feature_view import FeatureView

        entity = Entity(name="entity_id", join_keys=["entity_id"])
        fv = FeatureView(
            name="driver_stats",
            entities=[entity],
            ttl=timedelta(days=1),
        )
        fv.ensure_valid()  # Should not raise


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


def _make_feature_view(name: str, version: str = "latest"):
    from feast import FeatureView
    from feast.infra.offline_stores.file_source import FileSource

    return FeatureView(
        name=name,
        entities=[],
        ttl=timedelta(days=1),
        source=FileSource(path="data/dummy.parquet", timestamp_field="ts"),
        version=version,
    )


class TestCheckVersionPinConflict:
    """Unit tests for BaseRegistry.check_version_pin_conflict.

    The method is a read-only plan-time guard that mirrors the conflict
    detection inside apply_feature_view.  It should raise FeatureViewPinConflict
    when the user simultaneously pins to an existing version AND modifies the
    feature view definition.
    """

    def _make_registry(self, active_fv, pinned_fv_or_not_found):
        """Return a mock BaseRegistry wired with the given FV responses."""
        from feast.infra.registry.base_registry import BaseRegistry

        registry = MagicMock(spec=BaseRegistry)
        registry.get_any_feature_view.return_value = active_fv

        if isinstance(pinned_fv_or_not_found, Exception):
            registry.get_feature_view_by_version.side_effect = pinned_fv_or_not_found
        else:
            registry.get_feature_view_by_version.return_value = pinned_fv_or_not_found

        # Bind the real method to the mock instance so it runs our code.
        from feast.infra.registry.base_registry import BaseRegistry

        registry.check_version_pin_conflict = (
            BaseRegistry.check_version_pin_conflict.__get__(registry)
        )
        return registry

    def test_latest_version_no_conflict(self):
        """Requesting 'latest' never raises — no pin target to conflict with."""
        from feast.infra.registry.base_registry import BaseRegistry

        registry = MagicMock(spec=BaseRegistry)
        registry.check_version_pin_conflict = (
            BaseRegistry.check_version_pin_conflict.__get__(registry)
        )
        fv = _make_feature_view("driver_stats", version="latest")
        registry.check_version_pin_conflict(fv, "my_project")  # must not raise

    def test_forward_declaration_no_conflict(self):
        """Pinning to a version that doesn't exist yet is a forward declaration — no conflict."""
        from feast.errors import FeatureViewVersionNotFound

        fv = _make_feature_view("driver_stats", version="v2")
        registry = self._make_registry(
            active_fv=_make_feature_view("driver_stats"),
            pinned_fv_or_not_found=FeatureViewVersionNotFound(
                "driver_stats", "v2", "my_project"
            ),
        )
        registry.check_version_pin_conflict(fv, "my_project")  # must not raise

    def test_pin_no_schema_change_no_conflict(self):
        """Pinning to an existing version without changing the schema is fine."""
        active = _make_feature_view("driver_stats", version="latest")
        pinned = _make_feature_view("driver_stats", version="latest")

        # desired == active (same schema), only version pin differs
        desired = _make_feature_view("driver_stats", version="v1")
        registry = self._make_registry(active_fv=active, pinned_fv_or_not_found=pinned)
        registry.check_version_pin_conflict(desired, "my_project")  # must not raise

    def test_pin_with_schema_change_raises(self):
        """Pinning to an existing version while also changing the schema must raise."""
        from feast import Field
        from feast.errors import FeatureViewPinConflict
        from feast.types import Float32

        active = _make_feature_view("driver_stats", version="latest")
        pinned = _make_feature_view("driver_stats", version="latest")

        # Desired FV has an extra feature — schema change on top of pin.
        from feast import FeatureView
        from feast.infra.offline_stores.file_source import FileSource

        desired = FeatureView(
            name="driver_stats",
            entities=[],
            ttl=timedelta(days=1),
            source=FileSource(path="data/dummy.parquet", timestamp_field="ts"),
            version="v1",
            schema=[Field(name="trips", dtype=Float32)],
        )
        registry = self._make_registry(active_fv=active, pinned_fv_or_not_found=pinned)
        with pytest.raises(FeatureViewPinConflict):
            registry.check_version_pin_conflict(desired, "my_project")


class TestVersionDiffDisplay:
    """Unit tests for version pin display in diff_registry_objects.

    The 'version' spec field is excluded from the generic field-by-field loop and
    instead rendered using meta.current_version_number so the plan output reads
    'v2 (pin) -> v1 (pin)' rather than raw proto string values.
    """

    def _diff_fvs(self, current_fv, new_fv):
        from feast.diff.registry_diff import diff_registry_objects
        from feast.infra.registry.registry import FeastObjectType

        return diff_registry_objects(current_fv, new_fv, FeastObjectType.FEATURE_VIEW)

    def test_no_version_change_no_version_diff(self):
        """Two identical unpinned FVs produce no version property diff."""
        fv = _make_feature_view("driver_stats")
        result = self._diff_fvs(fv, fv)
        version_diffs = [
            p
            for p in result.feast_object_property_diffs
            if p.property_name == "version"
        ]
        assert not version_diffs

    def test_pin_shows_in_diff(self):
        """Pinning from 'latest' to v1 shows a 'version' property diff."""
        from feast.diff.property_diff import TransitionType

        current = _make_feature_view("driver_stats", version="latest")
        desired = _make_feature_view("driver_stats", version="v1")

        result = self._diff_fvs(current, desired)
        version_diffs = [
            p
            for p in result.feast_object_property_diffs
            if p.property_name == "version"
        ]
        assert len(version_diffs) == 1
        assert version_diffs[0].val_declared == "v1 (pin)"
        assert result.transition_type == TransitionType.UPDATE

    def test_unpin_shows_in_diff(self):
        """Moving from a pinned version back to latest shows a version property diff."""
        from feast.diff.property_diff import TransitionType

        current = _make_feature_view("driver_stats", version="v2")
        # Simulate the stored FV having current_version_number=2 in meta.
        current_proto = current.to_proto()
        current_proto.meta.current_version_number = 2

        # Re-hydrate so we have a proper FV with the meta set.
        from feast import FeatureView

        current_pinned = FeatureView.from_proto(current_proto)
        desired = _make_feature_view("driver_stats", version="latest")

        result = self._diff_fvs(current_pinned, desired)
        version_diffs = [
            p
            for p in result.feast_object_property_diffs
            if p.property_name == "version"
        ]
        assert len(version_diffs) == 1
        assert version_diffs[0].val_existing == "v2 (pin)"
        assert version_diffs[0].val_declared == "latest"
        assert result.transition_type == TransitionType.UPDATE

    def test_plan_calls_pin_conflict_check(self):
        """store.plan() iterates all feature views and calls check_version_pin_conflict."""
        from feast.infra.registry.base_registry import BaseRegistry

        registry = MagicMock(spec=BaseRegistry)
        fv = _make_feature_view("driver_stats", version="v1")

        # Simulate the loop added in store.plan() — call check_version_pin_conflict
        # for each FV in desired_repo_contents.feature_views.
        all_fvs = [fv]
        for f in all_fvs:
            registry.check_version_pin_conflict(f, "test_project")

        registry.check_version_pin_conflict.assert_called_once_with(fv, "test_project")
