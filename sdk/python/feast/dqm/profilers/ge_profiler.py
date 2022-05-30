import json
from types import FunctionType
from typing import Any, Callable, Dict, List

import dill
import great_expectations as ge
import numpy as np
import pandas as pd
from great_expectations.core import ExpectationSuite
from great_expectations.dataset import PandasDataset

from feast.dqm.profilers.profiler import (
    Profile,
    Profiler,
    ValidationError,
    ValidationReport,
)
from feast.protos.feast.core.ValidationProfile_pb2 import (
    GEValidationProfile as GEValidationProfileProto,
)
from feast.protos.feast.core.ValidationProfile_pb2 import (
    GEValidationProfiler as GEValidationProfilerProto,
)
from feast.protos.feast.serving.ServingService_pb2 import FieldStatus


def _prepare_dataset(dataset: PandasDataset) -> PandasDataset:
    dataset_copy = dataset.copy(deep=True)

    for column in dataset.columns:
        if pd.api.types.is_datetime64_any_dtype(dataset[column]):
            # GE cannot parse Timestamp or other pandas datetime time
            dataset_copy[column] = dataset[column].dt.strftime("%Y-%m-%dT%H:%M:%S")

        if dataset[column].dtype == np.float32:
            # GE converts expectation arguments into native Python float
            # This could cause error on comparison => so better to convert to double prematurely
            dataset_copy[column] = dataset[column].astype(np.float64)

        status_column = f"{column}__status"
        if status_column in dataset.columns:
            dataset_copy[column] = dataset_copy[column].mask(
                dataset[status_column] == FieldStatus.NOT_FOUND, np.nan
            )

    return dataset_copy


def _add_feature_metadata(dataset: PandasDataset) -> PandasDataset:
    for column in dataset.columns:
        if "__" not in column:
            # not a feature column
            continue

        if "event_timestamp" in dataset.columns:
            dataset[f"{column}__timestamp"] = dataset["event_timestamp"]

        dataset[f"{column}__status"] = FieldStatus.PRESENT
        dataset[f"{column}__status"] = dataset[f"{column}__status"].mask(
            dataset[column].isna(), FieldStatus.NOT_FOUND
        )

    return dataset


class GEProfile(Profile):
    """
    GEProfile is an implementation of abstract Profile for integration with Great Expectations.
    It executes validation by applying expectations from ExpectationSuite instance to a given dataset.
    """

    expectation_suite: ExpectationSuite

    def __init__(self, expectation_suite: ExpectationSuite):
        self.expectation_suite = expectation_suite

    def validate(self, df: pd.DataFrame) -> "GEValidationReport":
        """
        Validate provided dataframe against GE expectation suite.
        1. Pandas dataframe is converted into PandasDataset (GE type)
        2. Some fixes applied to the data to avoid crashes inside GE (see _prepare_dataset)
        3. Each expectation from ExpectationSuite instance tested against resulting dataset

        Return GEValidationReport, which parses great expectation's schema into list of generic ValidationErrors.
        """
        dataset = PandasDataset(df)

        dataset = _prepare_dataset(dataset)

        results = ge.validate(
            dataset, expectation_suite=self.expectation_suite, result_format="COMPLETE"
        )
        return GEValidationReport(results)

    def to_proto(self):
        return GEValidationProfileProto(
            expectation_suite=json.dumps(self.expectation_suite.to_json_dict()).encode()
        )

    @classmethod
    def from_proto(cls, proto: GEValidationProfileProto) -> "GEProfile":
        return GEProfile(
            expectation_suite=ExpectationSuite(**json.loads(proto.expectation_suite))
        )

    def __repr__(self):
        expectations = json.dumps(
            [e.to_json_dict() for e in self.expectation_suite.expectations], indent=2
        )
        return f"<GEProfile with expectations: {expectations}>"


class GEProfiler(Profiler):
    """
    GEProfiler is an implementation of abstract Profiler for integration with Great Expectations.
    It wraps around user defined profiler that should accept dataset (in a form of pandas dataframe)
    and return ExpectationSuite.
    """

    def __init__(
        self,
        user_defined_profiler: Callable[[pd.DataFrame], ExpectationSuite],
        with_feature_metadata: bool = False,
    ):
        self.user_defined_profiler = user_defined_profiler
        self.with_feature_metadata = with_feature_metadata

    def analyze_dataset(self, df: pd.DataFrame) -> Profile:
        """
        Generate GEProfile with ExpectationSuite (set of expectations)
        from a given pandas dataframe by applying user defined profiler.

        Some fixes are also applied to the dataset (see _prepare_dataset function) to make it compatible with GE.

        Return GEProfile
        """
        dataset = PandasDataset(df)

        dataset = _prepare_dataset(dataset)

        if self.with_feature_metadata:
            dataset = _add_feature_metadata(dataset)

        return GEProfile(expectation_suite=self.user_defined_profiler(dataset))

    def to_proto(self):
        # keep only the code and drop context for now
        # ToDo (pyalex): include some context, but not all (dill tries to pull too much)
        udp = FunctionType(self.user_defined_profiler.__code__, {})
        return GEValidationProfilerProto(
            profiler=GEValidationProfilerProto.UserDefinedProfiler(
                body=dill.dumps(udp, recurse=False)
            )
        )

    @classmethod
    def from_proto(cls, proto: GEValidationProfilerProto) -> "GEProfiler":
        return GEProfiler(user_defined_profiler=dill.loads(proto.profiler.body))


class GEValidationReport(ValidationReport):
    def __init__(self, validation_result: Dict[Any, Any]):
        self._validation_result = validation_result

    @property
    def is_success(self) -> bool:
        return self._validation_result["success"]

    @property
    def errors(self) -> List["ValidationError"]:
        return [
            ValidationError(
                check_name=res.expectation_config.expectation_type,
                column_name=res.expectation_config.kwargs["column"],
                check_config=res.expectation_config.kwargs,
                missing_count=res["result"].get("missing_count"),
                missing_percent=res["result"].get("missing_percent"),
                unexpected_count=res["result"].get("unexpected_count"),
                unexpected_percent=res["result"].get("unexpected_percent"),
            )
            for res in self._validation_result["results"]
            if not res["success"]
        ]

    def __repr__(self):
        failed_expectations = [
            res.to_json_dict()
            for res in self._validation_result["results"]
            if not res["success"]
        ]
        return json.dumps(failed_expectations, indent=2)


def ge_profiler(*args, with_feature_metadata=False):
    def wrapper(fun):
        return GEProfiler(
            user_defined_profiler=fun, with_feature_metadata=with_feature_metadata
        )

    if args:
        return wrapper(args[0])

    return wrapper
