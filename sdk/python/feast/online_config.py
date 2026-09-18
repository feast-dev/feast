# Copyright 2026 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from feast.protos.feast.core.FeatureView_pb2 import OnlineConfig as OnlineConfigProto


@dataclass
class OnlineConfig:
    """Online retention and write semantics for a feature view.

    Sequence mode is a declarative contract in this foundation release. Online
    stores must add explicit sequence support before these settings affect data
    storage or retrieval.
    """

    mode: str = "latest"
    max_length: Optional[int] = None
    max_age: Optional[timedelta] = None
    write_mode: str = "overwrite"

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Validate that the configuration represents supported semantics."""
        if self.mode not in {"latest", "sequence"}:
            raise ValueError("OnlineConfig mode must be either 'latest' or 'sequence'.")
        if self.write_mode not in {"overwrite", "append"}:
            raise ValueError(
                "OnlineConfig write_mode must be either 'overwrite' or 'append'."
            )

        if self.max_length is not None:
            if (
                isinstance(self.max_length, bool)
                or not isinstance(self.max_length, int)
                or self.max_length <= 0
            ):
                raise ValueError("OnlineConfig max_length must be a positive integer.")
            if self.max_length > 2_147_483_647:
                raise ValueError(
                    "OnlineConfig max_length must fit in a signed 32-bit integer."
                )

        if self.max_age is not None:
            if not isinstance(self.max_age, timedelta) or self.max_age <= timedelta(0):
                raise ValueError("OnlineConfig max_age must be a positive timedelta.")
            total_seconds = self.max_age.total_seconds()
            if not total_seconds.is_integer():
                raise ValueError(
                    "OnlineConfig max_age must use whole seconds because registry "
                    "serialization stores max_age_seconds."
                )

        if self.mode == "sequence":
            if self.write_mode != "append":
                raise ValueError(
                    "OnlineConfig mode='sequence' requires write_mode='append'."
                )
            if self.max_length is None:
                raise ValueError(
                    "OnlineConfig mode='sequence' requires max_length to be set."
                )
        else:
            if self.write_mode != "overwrite":
                raise ValueError(
                    "OnlineConfig mode='latest' requires write_mode='overwrite'."
                )
            if self.max_length is not None or self.max_age is not None:
                raise ValueError(
                    "OnlineConfig retention limits are only valid when mode='sequence'."
                )

    def to_proto(self) -> OnlineConfigProto:
        """Convert this configuration to its registry protobuf representation."""
        self.validate()
        return OnlineConfigProto(
            mode=(
                OnlineConfigProto.SEQUENCE
                if self.mode == "sequence"
                else OnlineConfigProto.LATEST
            ),
            max_length=self.max_length or 0,
            max_age_seconds=(
                int(self.max_age.total_seconds()) if self.max_age is not None else 0
            ),
            write_mode=(
                OnlineConfigProto.APPEND
                if self.write_mode == "append"
                else OnlineConfigProto.OVERWRITE
            ),
        )

    @classmethod
    def from_proto(cls, config_proto: OnlineConfigProto) -> OnlineConfig:
        """Create an OnlineConfig from its registry protobuf representation."""
        try:
            mode = OnlineConfigProto.Mode.Name(config_proto.mode).lower()
        except ValueError as e:
            raise ValueError(
                f"Unknown OnlineConfig mode enum value: {config_proto.mode}."
            ) from e
        try:
            write_mode = OnlineConfigProto.WriteMode.Name(
                config_proto.write_mode
            ).lower()
        except ValueError as e:
            raise ValueError(
                "Unknown OnlineConfig write_mode enum value: "
                f"{config_proto.write_mode}."
            ) from e

        return cls(
            mode=mode,
            max_length=config_proto.max_length or None,
            max_age=(
                timedelta(seconds=config_proto.max_age_seconds)
                if config_proto.max_age_seconds
                else None
            ),
            write_mode=write_mode,
        )
