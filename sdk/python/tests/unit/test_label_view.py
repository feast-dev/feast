from datetime import timedelta

import pytest

from feast.data_source import PushSource
from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.labeling import ConflictPolicy, LabelView
from feast.protos.feast.core.LabelView_pb2 import (
    ConflictResolutionPolicy as ConflictResolutionPolicyProto,
)
from feast.protos.feast.core.LabelView_pb2 import LabelView as LabelViewProto
from feast.types import Float32, String
from feast.value_type import ValueType


def _sample_label_view() -> LabelView:
    interaction = Entity(
        name="interaction",
        join_keys=["interaction_id"],
        value_type=ValueType.STRING,
    )
    label_push = PushSource(
        name="label_push",
        batch_source=FileSource(path="labels.parquet", timestamp_field="ts"),
    )
    return LabelView(
        name="interaction_labels",
        source=label_push,
        entities=[interaction],
        schema=[
            Field(name="interaction_id", dtype=String),
            Field(name="reward_label", dtype=String),
            Field(name="safety_score", dtype=Float32),
            Field(name="labeler", dtype=String),
        ],
        labeler_field="labeler",
        conflict_policy=ConflictPolicy.LAST_WRITE_WINS,
        retain_history=True,
        reference_feature_view="interaction_history",
        ttl=timedelta(days=90),
        online=True,
        description="Mutable reward labels",
        tags={"team": "safety"},
        owner="safety@example.com",
    )


class TestLabelViewCreation:
    def test_basic_creation(self):
        lv = _sample_label_view()
        assert lv.name == "interaction_labels"
        assert lv.entities == ["interaction"]
        assert len(lv.features) == 3
        assert len(lv.entity_columns) == 1
        assert lv.labeler_field == "labeler"
        assert lv.conflict_policy == ConflictPolicy.LAST_WRITE_WINS
        assert lv.retain_history is True
        assert lv.reference_feature_view == "interaction_history"
        assert lv.ttl == timedelta(days=90)
        assert lv.online is True
        assert lv.description == "Mutable reward labels"
        assert lv.tags == {"team": "safety"}
        assert lv.owner == "safety@example.com"

    def test_default_values(self):
        entity = Entity(name="item", join_keys=["item_id"], value_type=ValueType.STRING)
        lv = LabelView(
            name="minimal_labels",
            entities=[entity],
            schema=[
                Field(name="item_id", dtype=String),
                Field(name="quality_label", dtype=String),
            ],
        )
        assert lv.labeler_field == "labeler"
        assert lv.conflict_policy == ConflictPolicy.LAST_WRITE_WINS
        assert lv.retain_history is True
        assert lv.reference_feature_view == ""
        assert lv.ttl == timedelta(days=0)
        assert lv.online is True

    def test_conflicting_entity_join_keys_raises(self):
        e1 = Entity(name="e1", join_keys=["shared_key"])
        e2 = Entity(name="e2", join_keys=["shared_key"])
        with pytest.raises(ValueError, match="share a join key"):
            LabelView(
                name="bad",
                entities=[e1, e2],
                schema=[Field(name="shared_key", dtype=String)],
            )

    def test_feature_columns_exclude_entity_columns(self):
        lv = _sample_label_view()
        feature_names = {f.name for f in lv.features}
        entity_names = {f.name for f in lv.entity_columns}
        assert feature_names == {"reward_label", "safety_score", "labeler"}
        assert entity_names == {"interaction_id"}
        assert feature_names.isdisjoint(entity_names)


class TestLabelViewProtoRoundtrip:
    def test_to_proto(self):
        lv = _sample_label_view()
        proto = lv.to_proto()
        assert isinstance(proto, LabelViewProto)
        assert proto.spec.name == "interaction_labels"
        assert list(proto.spec.entities) == ["interaction"]
        assert len(proto.spec.features) == 3
        assert len(proto.spec.entity_columns) == 1
        assert proto.spec.labeler_field == "labeler"
        assert (
            proto.spec.conflict_policy == ConflictResolutionPolicyProto.LAST_WRITE_WINS
        )
        assert proto.spec.retain_history is True
        assert proto.spec.reference_feature_view == "interaction_history"
        assert proto.spec.online is True
        assert proto.spec.description == "Mutable reward labels"
        assert dict(proto.spec.tags) == {"team": "safety"}
        assert proto.spec.owner == "safety@example.com"

    def test_from_proto_roundtrip(self):
        lv = _sample_label_view()
        proto = lv.to_proto()
        lv2 = LabelView.from_proto(proto)

        assert lv2.name == lv.name
        assert lv2.entities == lv.entities
        assert sorted(f.name for f in lv2.features) == sorted(
            f.name for f in lv.features
        )
        assert sorted(f.name for f in lv2.entity_columns) == sorted(
            f.name for f in lv.entity_columns
        )
        assert lv2.labeler_field == lv.labeler_field
        assert lv2.conflict_policy == lv.conflict_policy
        assert lv2.retain_history == lv.retain_history
        assert lv2.reference_feature_view == lv.reference_feature_view
        assert lv2.ttl == lv.ttl
        assert lv2.online == lv.online
        assert lv2.description == lv.description
        assert lv2.tags == lv.tags
        assert lv2.owner == lv.owner

    def test_all_conflict_policies_roundtrip(self):
        entity = Entity(name="item", join_keys=["item_id"], value_type=ValueType.STRING)
        for policy in ConflictPolicy:
            lv = LabelView(
                name=f"lv_{policy.name}",
                entities=[entity],
                schema=[
                    Field(name="item_id", dtype=String),
                    Field(name="label", dtype=String),
                ],
                conflict_policy=policy,
            )
            proto = lv.to_proto()
            lv2 = LabelView.from_proto(proto)
            assert lv2.conflict_policy == policy


class TestLabelViewCopyAndEquality:
    def test_copy(self):
        lv = _sample_label_view()
        lv_copy = lv.__copy__()
        assert lv_copy == lv
        assert lv_copy is not lv
        assert lv_copy.features is not lv.features

    def test_equality_detects_differences(self):
        lv1 = _sample_label_view()
        lv2 = _sample_label_view()
        assert lv1 == lv2

        lv2_mod = lv2.__copy__()
        lv2_mod.labeler_field = "annotator"
        assert lv1 != lv2_mod

    def test_equality_type_check(self):
        lv = _sample_label_view()
        with pytest.raises(TypeError):
            lv == "not a label view"

    def test_hash_by_name(self):
        lv1 = _sample_label_view()
        lv2 = _sample_label_view()
        assert hash(lv1) == hash(lv2)


class TestLabelViewValidation:
    def test_ensure_valid_raises_on_empty_name(self):
        entity = Entity(name="e", join_keys=["eid"], value_type=ValueType.STRING)
        lv = LabelView(
            name="",
            entities=[entity],
            schema=[Field(name="eid", dtype=String)],
        )
        with pytest.raises(ValueError, match="needs a name"):
            lv.ensure_valid()

    def test_ensure_valid_raises_on_no_entities(self):
        lv = LabelView.__new__(LabelView)
        lv.name = "test"
        lv.entities = []
        lv.features = []
        lv.entity_columns = []
        lv.tags = {}
        with pytest.raises(ValueError, match="no entities"):
            lv.ensure_valid()


class TestLabelViewProtoClass:
    def test_proto_class_property(self):
        lv = _sample_label_view()
        assert lv.proto_class == LabelViewProto


class TestLabelViewInFeatureService:
    def test_label_view_in_feature_service(self):
        lv = _sample_label_view()
        fs = FeatureService(
            name="training_service",
            features=[lv],
        )
        assert len(fs.feature_view_projections) == 1
        assert fs.feature_view_projections[0].name == "interaction_labels"


class TestLabelViewRegistryRoundtrip:
    """Tests that LabelView can be applied/retrieved from the file-based registry proto."""

    def test_registry_proto_roundtrip(self):
        from feast.infra.registry.proto_registry_utils import (
            get_label_view,
            list_label_views,
        )
        from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto

        lv = _sample_label_view()
        proto = lv.to_proto()
        proto.spec.project = "test_project"

        registry = RegistryProto()
        registry.label_views.append(proto)

        # list
        views = list_label_views(registry, "test_project", None)
        assert len(views) == 1
        assert views[0].name == "interaction_labels"
        assert views[0].labeler_field == "labeler"
        assert views[0].conflict_policy == ConflictPolicy.LAST_WRITE_WINS

        # get
        lv_got = get_label_view(registry, "interaction_labels", "test_project")
        assert lv_got.name == "interaction_labels"
        assert lv_got.retain_history is True

    def test_registry_proto_get_not_found(self):
        from feast.errors import FeatureViewNotFoundException
        from feast.infra.registry.proto_registry_utils import get_label_view
        from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto

        registry = RegistryProto()
        with pytest.raises(FeatureViewNotFoundException):
            get_label_view(registry, "nonexistent", "test_project")

    def test_list_all_feature_views_includes_label_views(self):
        from feast.infra.registry.proto_registry_utils import list_all_feature_views
        from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto

        lv = _sample_label_view()
        proto = lv.to_proto()
        proto.spec.project = "test_project"

        registry = RegistryProto()
        registry.label_views.append(proto)

        all_views = list_all_feature_views(registry, "test_project", None)
        assert any(isinstance(v, LabelView) for v in all_views)

    def test_get_any_feature_view_finds_label_view(self):
        from feast.infra.registry.proto_registry_utils import get_any_feature_view
        from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto

        lv = _sample_label_view()
        proto = lv.to_proto()
        proto.spec.project = "test_project"

        registry = RegistryProto()
        registry.label_views.append(proto)

        found = get_any_feature_view(registry, "interaction_labels", "test_project")
        assert isinstance(found, LabelView)
        assert found.name == "interaction_labels"

    def test_repo_contents_includes_label_views(self):
        from feast.repo_contents import RepoContents

        lv = _sample_label_view()
        rc = RepoContents(
            projects=[],
            data_sources=[],
            feature_views=[],
            on_demand_feature_views=[],
            stream_feature_views=[],
            label_views=[lv],
            entities=[],
            feature_services=[],
            permissions=[],
        )
        reg_proto = rc.to_registry_proto()
        assert len(reg_proto.label_views) == 1
        assert reg_proto.label_views[0].spec.name == "interaction_labels"

    def test_feast_object_type_label_view(self):
        from feast.infra.registry.registry import FeastObjectType

        assert FeastObjectType.LABEL_VIEW.value == "label view"


class TestLabelViewBatchSourceProperty:
    """Tests for the batch_source / stream_source compatibility properties."""

    def test_batch_source_from_push_source(self):
        lv = _sample_label_view()
        assert lv.batch_source is not None
        assert isinstance(lv.batch_source, FileSource)

    def test_stream_source_from_push_source(self):
        lv = _sample_label_view()
        assert lv.stream_source is not None
        assert isinstance(lv.stream_source, PushSource)

    def test_batch_source_from_file_source(self):
        entity = Entity(name="item", join_keys=["item_id"], value_type=ValueType.STRING)
        fs = FileSource(path="data.parquet", timestamp_field="ts")
        lv = LabelView(
            name="file_labels",
            entities=[entity],
            schema=[
                Field(name="item_id", dtype=String),
                Field(name="label", dtype=String),
            ],
            source=fs,
        )
        assert lv.batch_source is fs
        assert lv.stream_source is None

    def test_batch_source_none_when_no_source(self):
        entity = Entity(name="item", join_keys=["item_id"], value_type=ValueType.STRING)
        lv = LabelView(
            name="no_source_labels",
            entities=[entity],
            schema=[
                Field(name="item_id", dtype=String),
                Field(name="label", dtype=String),
            ],
        )
        assert lv.batch_source is None
        assert lv.stream_source is None


class TestLabelViewFeatureStoreIntegration:
    """Tests for FeatureStore integration without needing a real store."""

    def test_label_view_in_feature_views_list_includes_type(self):
        from feast.feature_view import FeatureView as FV
        from feast.on_demand_feature_view import OnDemandFeatureView as ODFV

        lv = _sample_label_view()
        assert not isinstance(lv, FV)
        assert not isinstance(lv, ODFV)

    def test_validate_feature_views_catches_name_conflict(self):
        from feast.feature_store import _validate_feature_views

        entity = Entity(name="item", join_keys=["item_id"], value_type=ValueType.STRING)
        lv1 = LabelView(
            name="my_view",
            entities=[entity],
            schema=[
                Field(name="item_id", dtype=String),
                Field(name="label", dtype=String),
            ],
        )
        lv2 = LabelView(
            name="MY_VIEW",
            entities=[entity],
            schema=[
                Field(name="item_id", dtype=String),
                Field(name="label", dtype=String),
            ],
        )
        from feast.errors import ConflictingFeatureViewNames

        with pytest.raises(ConflictingFeatureViewNames):
            _validate_feature_views([lv1, lv2])

    def test_materialization_task_accepts_label_view(self):
        from datetime import datetime

        from feast.infra.common.materialization_job import MaterializationTask

        lv = _sample_label_view()
        task = MaterializationTask(
            project="test",
            feature_view=lv,
            start_time=datetime(2025, 1, 1),
            end_time=datetime(2025, 1, 2),
        )
        assert task.feature_view.name == "interaction_labels"


class TestConflictPolicy:
    def test_proto_roundtrip(self):
        for policy in ConflictPolicy:
            proto_val = policy.to_proto()
            assert ConflictPolicy.from_proto(proto_val) == policy

    def test_enum_values(self):
        assert ConflictPolicy.LAST_WRITE_WINS.value == "last_write_wins"
        assert ConflictPolicy.LABELER_PRIORITY.value == "labeler_priority"
        assert ConflictPolicy.MAJORITY_VOTE.value == "majority_vote"
