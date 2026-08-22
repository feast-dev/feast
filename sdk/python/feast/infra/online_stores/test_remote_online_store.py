from datetime import datetime
from unittest.mock import MagicMock, patch
from feast.feature_view import FeatureView
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.remote import RemoteOnlineStore

def test_remote_online_write_batch_custom_timestamp_columns():
    source = FileSource(
        path="dummy.parquet",
        timestamp_field="custom_event_ts",
        created_timestamp_column="created_at",
    )
    fv = FeatureView(
        name="test_fv",
        source=source,
        entities=[],
        schema=[],
    )

    remote_store = RemoteOnlineStore()
    config = MagicMock()

    with patch("feast.infra.online_stores.remote.post_remote_online_write") as mock_post:
        data = [
            (MagicMock(join_keys=[], entity_values=[]), {}, datetime.utcnow(), datetime.utcnow())
        ]
        remote_store.online_write_batch(
            config=config,
            table=fv,
            data=data,
            progress=None,
        )

        args, kwargs = mock_post.call_args
        req_df = kwargs["req_body"]["df"]

        # Assert custom field names are used in the payload
        assert "custom_event_ts" in req_df
        assert "created_at" in req_df
        assert "event_timestamp" not in req_df
        assert "created" not in req_df
