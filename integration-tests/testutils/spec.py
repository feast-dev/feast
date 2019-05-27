import yaml
import numpy as np
from glob import glob


def get_entity_name(entity_spec_file):
    with open(entity_spec_file, "rb") as f:
        entity_spec = yaml.safe_load(f)
    assert "name" in entity_spec
    return entity_spec["name"]


def get_feature_infos(feature_specs_files):
    value_type_to_dtype = {
        "INT32": np.int32,
        "INT64": np.int64,
        "DOUBLE": np.float64,
        "FLOAT": np.float64,
    }
    value_type_to_pandas_dtype = {
        "INT32": "Int32",
        "INT64": "Int64",
        "DOUBLE": np.float64,
        "FLOAT": np.float64,
    }
    feature_infos = []
    for path_name in sorted(glob(feature_specs_files)):
        with open(path_name, "rb") as f:
            feature_spec = yaml.safe_load(f)
        assert feature_spec["valueType"] in value_type_to_dtype
        feature_infos.append(
            {
                "id": feature_spec["id"],
                "name": feature_spec["name"],
                "dtype": value_type_to_dtype[feature_spec["valueType"]],
                "pandas_dtype": value_type_to_pandas_dtype[feature_spec["valueType"]]
            }
        )
    assert len(feature_infos) > 0
    return feature_infos
