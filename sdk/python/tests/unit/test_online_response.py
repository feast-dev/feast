from datetime import datetime, timezone

from google.protobuf.timestamp_pb2 import Timestamp

from feast.online_response import OnlineResponse
from feast.protos.feast.serving.ServingService_pb2 import (
    FieldStatus,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.Value_pb2 import Value as ValueProto


def test_online_response_include_created_timestamps():
    # Construct a sample GetOnlineFeaturesResponse proto
    event_ts = Timestamp()
    event_ts.FromDatetime(datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc))

    created_ts = Timestamp()
    created_ts.FromDatetime(datetime(2026, 1, 1, 12, 5, 0, tzinfo=timezone.utc))

    proto = GetOnlineFeaturesResponse()
    proto.metadata.feature_names.val.append("driver_fv:conv_rate")

    proto.results.append(
        GetOnlineFeaturesResponse.FeatureVector(
            values=[ValueProto(float_val=0.85)],
            statuses=[FieldStatus.PRESENT],
            event_timestamps=[event_ts],
            created_timestamps=[created_ts],
        )
    )

    response = OnlineResponse(proto)

    # 1. Test dictionary with both timestamps enabled
    res_dict = response.to_dict(
        include_event_timestamps=True,
        include_created_timestamps=True,
    )
    assert "driver_fv:conv_rate" in res_dict
    assert "driver_fv:conv_rate__ts" in res_dict
    assert "driver_fv:conv_rate__created_timestamp" in res_dict
    assert res_dict["driver_fv:conv_rate__created_timestamp"] == [created_ts.seconds]

    # 2. Test DataFrame with both timestamps enabled
    df = response.to_df(
        include_event_timestamps=True,
        include_created_timestamps=True,
    )
    assert "driver_fv:conv_rate__created_timestamp" in df.columns
    assert df["driver_fv:conv_rate__created_timestamp"].iloc[0] == created_ts.seconds
