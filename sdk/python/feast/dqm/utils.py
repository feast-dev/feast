from typing import TYPE_CHECKING, List, Optional

import pandas as pd
import pyarrow

from feast.dqm.errors import ValidationFailed
from feast.dqm.profilers.profiler import Profile
from feast.infra.offline_stores.offline_store import RetrievalJob, RetrievalMetadata
from feast.saved_dataset import SavedDatasetStorage, ValidationReference

if TYPE_CHECKING:
    from feast.feature_store import FeatureStore
    from feast.on_demand_feature_view import OnDemandFeatureView


class RetrievalJobWithValidation(RetrievalJob):
    def __init__(
        self,
        retrieval_job: RetrievalJob,
        validation_reference: ValidationReference,
        feature_store: "FeatureStore",
    ):
        self._retrieval_job = retrieval_job
        self._validation_reference = validation_reference
        self._feature_store = feature_store

    def to_df(self) -> pd.DataFrame:
        df = self._retrieval_job.to_df()

        profile = get_reference_profile(self._feature_store, self._validation_reference)
        validation_result = profile.validate(df)
        if not validation_result.is_success:
            raise ValidationFailed(validation_result)

        return df

    def to_arrow(self) -> pyarrow.Table:
        table = self._retrieval_job.to_arrow()

        profile = get_reference_profile(self._feature_store, self._validation_reference)
        validation_result = profile.validate(table.to_pandas())
        if not validation_result.is_success:
            raise ValidationFailed(validation_result)

        return table

    @property
    def full_feature_names(self) -> bool:
        return self._retrieval_job.full_feature_names

    @property
    def on_demand_feature_views(self) -> Optional[List["OnDemandFeatureView"]]:
        return self._retrieval_job.on_demand_feature_views

    def _to_df_internal(self) -> pd.DataFrame:
        raise NotImplementedError

    def _to_arrow_internal(self) -> pyarrow.Table:
        raise NotImplementedError

    def persist(self, storage: SavedDatasetStorage) -> "RetrievalJob":
        return self._retrieval_job.persist(storage)

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._retrieval_job.metadata


def get_reference_profile(
    fs: "FeatureStore", reference: ValidationReference
) -> Profile:
    dataset = fs.get_saved_dataset(reference.dataset.name)
    return reference.profiler.analyze_dataset(dataset.to_df())
