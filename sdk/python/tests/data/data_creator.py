import io
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd

from feast.types import FeastType, Float32, Int32, Int64, String
from feast.utils import _utc_now


def create_basic_driver_dataset(
    entity_type: FeastType = Int32,
    feature_dtype: Optional[str] = None,
    feature_is_list: bool = False,
    list_has_empty_list: bool = False,
) -> pd.DataFrame:
    now = _utc_now().replace(microsecond=0, second=0, minute=0)
    ts = pd.Timestamp(now).round("ms")
    data = {
        "driver_id": get_entities_for_feast_type(entity_type),
        "value": get_feature_values_for_dtype(
            feature_dtype, feature_is_list, list_has_empty_list
        ),
        "ts_1": [
            ts - timedelta(hours=4),
            ts,
            ts - timedelta(hours=3),
            # Use different time zones to test tz-naive -> tz-aware conversion
            (ts - timedelta(hours=4))
            .replace(tzinfo=timezone.utc)
            .astimezone(tz=ZoneInfo("Europe/Berlin")),
            (ts - timedelta(hours=1))
            .replace(tzinfo=timezone.utc)
            .astimezone(tz=ZoneInfo("US/Pacific")),
        ],
        "created_ts": [ts, ts, ts, ts, ts],
    }
    return pd.DataFrame.from_dict(data)


def get_entities_for_feast_type(feast_type: FeastType) -> List:
    feast_type_map: Dict[FeastType, List] = {
        Int32: [1, 2, 1, 3, 3],
        Int64: [1, 2, 1, 3, 3],
        Float32: [1.0, 2.0, 1.0, 3.0, 3.0],
        String: ["1", "2", "1", "3", "3"],
    }
    return feast_type_map[feast_type]


def get_feature_values_for_dtype(
    dtype: Optional[str], is_list: bool, has_empty_list: bool
) -> List:
    if dtype is None:
        return [0.1, None, 0.3, 4, 5]
    # TODO(adchia): for int columns, consider having a better error when dealing with None values (pandas int dfs can't
    #  have na)
    dtype_map: Dict[str, List] = {
        "int32": [1, 2, 3, 4, 5],
        "int64": [1, 2, 3, 4, 5],
        "float": [1.0, None, 3.0, 4.0, 5.0],
        "string": ["1", None, "3", "4", "5"],
        "bytes": [b"1", None, b"3", b"4", b"5"],
        "bool": [True, None, False, True, False],
        "datetime": [
            datetime(1980, 1, 1),
            None,
            datetime(1981, 1, 1),
            datetime(1982, 1, 1),
            datetime(1982, 1, 1),
        ],
    }
    non_list_val = dtype_map[dtype]
    if is_list:
        # TODO: Add test where all lists are empty and type inference is expected to fail.
        if has_empty_list:
            # Need at least one non-empty element for type inference
            return [[] for n in non_list_val[:-1]] + [non_list_val[-1:]]
        return [[n, n] if n is not None else None for n in non_list_val]
    else:
        return non_list_val


def create_document_dataset() -> pd.DataFrame:
    data = {
        "item_id": [1, 2, 3],
        "string_feature": ["a", "b", "c"],
        "float_feature": [1.0, 2.0, 3.0],
        "embedding_float": [[4.0, 5.0], [1.0, 2.0], [3.0, 4.0]],
        "embedding_double": [[4.0, 5.0], [1.0, 2.0], [3.0, 4.0]],
        "ts": [
            pd.Timestamp(_utc_now()).round("ms"),
            pd.Timestamp(_utc_now()).round("ms"),
            pd.Timestamp(_utc_now()).round("ms"),
        ],
        "created_ts": [
            pd.Timestamp(_utc_now()).round("ms"),
            pd.Timestamp(_utc_now()).round("ms"),
            pd.Timestamp(_utc_now()).round("ms"),
        ],
    }
    return pd.DataFrame(data)


def create_image_dataset() -> pd.DataFrame:
    """Create a dataset with image data for testing image search functionality."""

    def create_test_image_bytes(color=(255, 0, 0), size=(32, 32)):
        """Create synthetic image bytes for testing."""
        try:
            from PIL import Image

            img = Image.new("RGB", size, color=color)
            img_bytes = io.BytesIO()
            img.save(img_bytes, format="JPEG")
            return img_bytes.getvalue()
        except ImportError:
            # Return dummy bytes if PIL not available
            return b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x01\x00H\x00H\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t\x08\n\x0c\x14\r\x0c\x0b\x0b\x0c\x19\x12\x13\x0f\x14\x1d\x1a\x1f\x1e\x1d\x1a\x1c\x1c $.' \",#\x1c\x1c(7),01444\x1f'9=82<.342\xff\xc0\x00\x11\x08\x00 \x00 \x01\x01\x11\x00\x02\x11\x01\x03\x11\x01\xff\xc4\x00\x14\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\xff\xc4\x00\x14\x10\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xda\x00\x0c\x03\x01\x00\x02\x11\x03\x11\x00\x3f\x00\xaa\xff\xd9"

    data = {
        "item_id": [1, 2, 3],
        "image_filename": ["red_image.jpg", "green_image.jpg", "blue_image.jpg"],
        "image_bytes": [
            create_test_image_bytes((255, 0, 0)),  # Red
            create_test_image_bytes((0, 255, 0)),  # Green
            create_test_image_bytes((0, 0, 255)),  # Blue
        ],
        "image_embedding": [
            [0.9, 0.1],  # Red-ish embedding
            [0.2, 0.8],  # Green-ish embedding
            [0.1, 0.9],  # Blue-ish embedding
        ],
        "category": ["primary", "primary", "primary"],
        "description": [
            "A red colored image",
            "A green colored image",
            "A blue colored image",
        ],
        "ts": [
            pd.Timestamp(_utc_now()).round("ms"),
            pd.Timestamp(_utc_now()).round("ms"),
            pd.Timestamp(_utc_now()).round("ms"),
        ],
        "created_ts": [
            pd.Timestamp(_utc_now()).round("ms"),
            pd.Timestamp(_utc_now()).round("ms"),
            pd.Timestamp(_utc_now()).round("ms"),
        ],
    }
    return pd.DataFrame(data)
