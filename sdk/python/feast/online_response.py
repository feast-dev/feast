# Copyright 2020 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import TYPE_CHECKING, Any, Dict, List, TypeAlias, Union

import pandas as pd
import pyarrow as pa

from feast.feature_view import DUMMY_ENTITY_ID
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesResponse
from feast.torch_wrapper import get_torch
from feast.type_map import feast_value_type_to_python_type

if TYPE_CHECKING:
    import torch

    TorchTensor: TypeAlias = torch.Tensor
else:
    TorchTensor: TypeAlias = Any

TIMESTAMP_POSTFIX: str = "__ts"


class OnlineResponse:
    """
    Defines an online response in feast.
    """

    def __init__(self, online_response_proto: GetOnlineFeaturesResponse):
        """
        Construct a native online response from its protobuf version.

        Args:
        online_response_proto: GetOnlineResponse proto object to construct from.
        """
        self.proto = online_response_proto
        # Delete DUMMY_ENTITY_ID from proto if it exists
        for idx, val in enumerate(self.proto.metadata.feature_names.val):
            if val == DUMMY_ENTITY_ID:
                del self.proto.metadata.feature_names.val[idx]
                del self.proto.results[idx]

                break

    def to_dict(self, include_event_timestamps: bool = False) -> Dict[str, Any]:
        """
        Converts GetOnlineFeaturesResponse features into a dictionary form.

        Args:
        include_event_timestamps: bool Optionally include feature timestamps in the dictionary
        """
        response: Dict[str, List[Any]] = {}

        for feature_ref, feature_vector in zip(
            self.proto.metadata.feature_names.val, self.proto.results
        ):
            response[feature_ref] = [
                feast_value_type_to_python_type(v) for v in feature_vector.values
            ]

            if include_event_timestamps:
                timestamp_ref = feature_ref + TIMESTAMP_POSTFIX
                response[timestamp_ref] = [
                    ts.seconds for ts in feature_vector.event_timestamps
                ]

        return response

    def to_df(self, include_event_timestamps: bool = False) -> pd.DataFrame:
        """
        Converts GetOnlineFeaturesResponse features into Panda dataframe form.

        Args:
        include_event_timestamps: bool Optionally include feature timestamps in the dataframe
        """

        return pd.DataFrame(self.to_dict(include_event_timestamps))

    def to_arrow(self, include_event_timestamps: bool = False) -> pa.Table:
        """
        Converts GetOnlineFeaturesResponse features into pyarrow Table.

        Args:
        include_event_timestamps: bool Optionally include feature timestamps in the table
        """

        return pa.Table.from_pydict(self.to_dict(include_event_timestamps))

    def to_tensor(
        self,
        kind: str = "torch",
        default_value: Any = float("nan"),
    ) -> Dict[str, Union[TorchTensor, List[Any]]]:
        """
        Converts GetOnlineFeaturesResponse features into a dictionary of tensors or lists.

        - Numeric features (int, float, bool) -> torch.Tensor
        - Non-numeric features (e.g., strings) -> list[Any]

        Args:
            kind: Backend tensor type. Currently only "torch" is supported.
            default_value: Value to substitute for missing (None) entries.

        Returns:
            Dict[str, Union[torch.Tensor, List[Any]]]: Mapping of feature names to tensors or lists.
        """
        if kind != "torch":
            raise ValueError(
                f"Unsupported tensor kind: {kind}. Only 'torch' is supported currently."
            )
        torch = get_torch()
        feature_dict = self.to_dict(include_event_timestamps=False)
        feature_keys = set(self.proto.metadata.feature_names.val)
        tensor_dict: Dict[str, Union[TorchTensor, List[Any]]] = {}
        for key in feature_keys:
            raw_values = feature_dict[key]
            values = [v if v is not None else default_value for v in raw_values]
            first_valid = next((v for v in values if v is not None), None)
            if isinstance(first_valid, (int, float, bool)):
                try:
                    device = "cuda" if torch.cuda.is_available() else "cpu"
                    tensor_dict[key] = torch.tensor(values, device=device)
                except Exception as e:
                    raise ValueError(
                        f"Failed to convert values for '{key}' to tensor: {e}"
                    )
            else:
                tensor_dict[key] = (
                    values  # Return as-is for strings or unsupported types
                )
        return tensor_dict
