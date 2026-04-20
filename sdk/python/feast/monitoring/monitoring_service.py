import logging
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from feast.feature_logging import LOG_TIMESTAMP_FIELD, FeatureServiceLoggingSource
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.monitoring.dqm_job_manager import DQMJobManager
from feast.monitoring.metrics_calculator import MetricsCalculator

logger = logging.getLogger(__name__)

VALID_GRANULARITIES = OfflineStore.MONITORING_VALID_GRANULARITIES

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_FAR_FUTURE = datetime(2099, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

GRANULARITY_WINDOWS = {
    "daily": timedelta(days=1),
    "weekly": timedelta(days=7),
    "biweekly": timedelta(days=14),
    "monthly": timedelta(days=30),
    "quarterly": timedelta(days=90),
}


class MonitoringService:
    def __init__(self, store: "FeatureStore"):  # noqa: F821
        self._store = store
        self._job_manager: Optional[DQMJobManager] = None
        self._calculator = MetricsCalculator()
        self._monitoring_tables_ensured = False

    def _get_offline_store(self):
        return self._store._get_provider().offline_store

    def _ensure_monitoring_tables(self):
        if not self._monitoring_tables_ensured:
            self._get_offline_store().ensure_monitoring_tables(self._store.config)
            self._monitoring_tables_ensured = True

    @property
    def job_manager(self) -> DQMJobManager:
        if self._job_manager is None:
            offline_store_config = self._store.config.offline_store
            self._job_manager = DQMJobManager(offline_store_config)
            self._job_manager.ensure_table()
        return self._job_manager

    # ------------------------------------------------------------------ #
    #  Auto-compute: detect dates, compute all granularities
    # ------------------------------------------------------------------ #

    def auto_compute(
        self,
        project: Optional[str] = None,
        feature_view_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Detect date ranges from source data and compute all granularities."""
        start_time = time.time()
        self._ensure_monitoring_tables()
        if project is None:
            project = self._store.config.project

        feature_views = self._resolve_feature_views(project, feature_view_name)
        total_features = 0
        total_views = 0
        granularities_computed = set()

        for fv in feature_views:
            try:
                feature_fields = self._classify_fields(fv)
                if not feature_fields:
                    continue

                max_ts = self._get_max_timestamp(fv)
                if max_ts is None:
                    logger.warning(
                        "No data found for feature view '%s', skipping", fv.name
                    )
                    continue

                now = datetime.now(timezone.utc)

                for granularity, window in GRANULARITY_WINDOWS.items():
                    window_start = max_ts - window
                    metrics_list = self._compute_feature_metrics(
                        fv,
                        feature_fields,
                        window_start,
                        max_ts,
                    )
                    self._save_computed_metrics(
                        project=project,
                        feature_view=fv,
                        metrics_list=metrics_list,
                        metric_date=window_start.date(),
                        granularity=granularity,
                        set_baseline=False,
                        now=now,
                    )
                    total_features += len(metrics_list)
                    granularities_computed.add(granularity)

                self._compute_feature_service_metrics(
                    project=project,
                    granularity="daily",
                    metric_dates=[max_ts.date() - timedelta(days=1)],
                    set_baseline=False,
                )
                total_views += 1
            except Exception:
                logger.exception(
                    "Failed to auto-compute metrics for feature view '%s'", fv.name
                )

        duration_ms = int((time.time() - start_time) * 1000)

        return {
            "status": "completed",
            "computed_feature_views": total_views,
            "computed_features": total_features,
            "granularities": sorted(granularities_computed),
            "duration_ms": duration_ms,
        }

    # ------------------------------------------------------------------ #
    #  Log source: compute metrics from feature serving logs
    # ------------------------------------------------------------------ #

    def compute_log_metrics(
        self,
        project: str,
        feature_service_name: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        granularity: str = "daily",
        set_baseline: bool = False,
    ) -> Dict[str, Any]:
        """Compute monitoring metrics from feature serving logs.

        Requires the feature service to have a logging_config with a
        LoggingDestination that can be converted to a DataSource.
        """
        self._ensure_monitoring_tables()
        if granularity not in VALID_GRANULARITIES:
            raise ValueError(
                f"Invalid granularity '{granularity}'. "
                f"Must be one of {VALID_GRANULARITIES}"
            )

        start_time = time.time()
        start_dt, end_dt = self._to_date_range(start_date, end_date)

        if project is None:
            project = self._store.config.project

        fs = self._store.registry.get_feature_service(
            name=feature_service_name, project=project
        )
        log_source = self._resolve_log_source(fs)
        if log_source is None:
            return {
                "status": "skipped",
                "reason": f"Feature service '{feature_service_name}' has no logging configured",
                "duration_ms": int((time.time() - start_time) * 1000),
            }

        data_source, ts_field, feature_fields, log_col_map = log_source
        metrics_list = self._compute_from_source(
            data_source,
            ts_field,
            feature_fields,
            start_dt,
            end_dt,
        )

        now = datetime.now(timezone.utc)
        metric_date = start_dt.date()

        self._save_log_metrics(
            project=project,
            feature_service_name=feature_service_name,
            log_col_map=log_col_map,
            metrics_list=metrics_list,
            metric_date=metric_date,
            granularity=granularity,
            set_baseline=set_baseline,
            now=now,
        )

        duration_ms = int((time.time() - start_time) * 1000)
        return {
            "status": "completed",
            "data_source_type": "log",
            "feature_service_name": feature_service_name,
            "granularity": granularity,
            "computed_features": len(metrics_list),
            "metric_date": metric_date.isoformat(),
            "duration_ms": duration_ms,
        }

    def auto_compute_log_metrics(
        self,
        project: Optional[str] = None,
        feature_service_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Auto-detect date ranges from log data and compute all granularities."""
        start_time = time.time()
        self._ensure_monitoring_tables()
        if project is None:
            project = self._store.config.project

        if feature_service_name:
            services = [
                self._store.registry.get_feature_service(
                    name=feature_service_name, project=project
                )
            ]
        else:
            services = self._store.registry.list_feature_services(project=project)

        total_features = 0
        total_services = 0
        granularities_computed: set = set()

        for fs in services:
            try:
                log_source = self._resolve_log_source(fs)
                if log_source is None:
                    continue

                data_source, ts_field, feature_fields, log_col_map = log_source

                max_ts = self._get_max_timestamp_for_source(data_source, ts_field)
                if max_ts is None:
                    logger.warning(
                        "No log data found for feature service '%s', skipping",
                        fs.name,
                    )
                    continue

                now = datetime.now(timezone.utc)

                for gran, window in GRANULARITY_WINDOWS.items():
                    window_start = max_ts - window
                    metrics_list = self._compute_from_source(
                        data_source,
                        ts_field,
                        feature_fields,
                        window_start,
                        max_ts,
                    )
                    self._save_log_metrics(
                        project=project,
                        feature_service_name=fs.name,
                        log_col_map=log_col_map,
                        metrics_list=metrics_list,
                        metric_date=window_start.date(),
                        granularity=gran,
                        set_baseline=False,
                        now=now,
                    )
                    total_features += len(metrics_list)
                    granularities_computed.add(gran)

                total_services += 1
            except Exception:
                logger.exception(
                    "Failed to auto-compute log metrics for feature service '%s'",
                    fs.name,
                )

        duration_ms = int((time.time() - start_time) * 1000)
        return {
            "status": "completed",
            "data_source_type": "log",
            "computed_feature_services": total_services,
            "computed_features": total_features,
            "granularities": sorted(granularities_computed),
            "duration_ms": duration_ms,
        }

    # ------------------------------------------------------------------ #
    #  Baseline: compute from all available source data
    # ------------------------------------------------------------------ #

    def compute_baseline(
        self,
        project: Optional[str] = None,
        feature_view_name: Optional[str] = None,
        feature_names: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Compute baseline metrics from all available source data.

        Idempotent: only features without existing baselines are computed.
        """
        start_time = time.time()
        self._ensure_monitoring_tables()
        if project is None:
            project = self._store.config.project

        feature_views = self._resolve_feature_views(project, feature_view_name)
        total_features = 0
        total_views = 0

        for fv in feature_views:
            try:
                fields_needing_baseline = self._get_features_without_baseline(
                    project, fv, feature_names
                )
                if not fields_needing_baseline:
                    logger.info(
                        "All features in '%s' already have baselines, skipping",
                        fv.name,
                    )
                    continue

                feature_fields = self._classify_fields(
                    fv, fields=fields_needing_baseline
                )
                if not feature_fields:
                    continue

                metrics_list = self._compute_feature_metrics(
                    fv,
                    feature_fields,
                    _EPOCH,
                    _FAR_FUTURE,
                )

                now = datetime.now(timezone.utc)
                offline_store = self._get_offline_store()
                offline_store.clear_monitoring_baseline(
                    config=self._store.config,
                    project=project,
                    feature_view_name=fv.name,
                )

                self._save_computed_metrics(
                    project=project,
                    feature_view=fv,
                    metrics_list=metrics_list,
                    metric_date=date.today(),
                    granularity="daily",
                    set_baseline=True,
                    now=now,
                )

                total_features += len(metrics_list)
                total_views += 1
            except Exception:
                logger.exception(
                    "Failed to compute baseline for feature view '%s'", fv.name
                )

        duration_ms = int((time.time() - start_time) * 1000)

        return {
            "status": "completed",
            "computed_features": total_features,
            "computed_feature_views": total_views,
            "is_baseline": True,
            "duration_ms": duration_ms,
        }

    # ------------------------------------------------------------------ #
    #  Compute: explicit dates + granularity (stored)
    # ------------------------------------------------------------------ #

    def compute_metrics(
        self,
        project: str,
        feature_view_name: Optional[str] = None,
        feature_names: Optional[List[str]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        granularity: str = "daily",
        set_baseline: bool = False,
    ) -> Dict[str, Any]:
        self._ensure_monitoring_tables()
        if granularity not in VALID_GRANULARITIES:
            raise ValueError(
                f"Invalid granularity '{granularity}'. "
                f"Must be one of {VALID_GRANULARITIES}"
            )

        start_time = time.time()
        start_dt, end_dt = self._to_date_range(start_date, end_date)

        feature_views = self._resolve_feature_views(project, feature_view_name)

        total_features = 0
        total_views = 0
        computed_dates: set = set()

        for fv in feature_views:
            try:
                fv_metrics = self._compute_for_feature_view(
                    project=project,
                    feature_view=fv,
                    feature_names=feature_names,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    granularity=granularity,
                    set_baseline=set_baseline,
                )
                total_features += fv_metrics["feature_count"]
                total_views += 1
                computed_dates.update(fv_metrics["dates"])
            except Exception:
                logger.exception(
                    "Failed to compute metrics for feature view '%s'", fv.name
                )

        total_services = self._compute_feature_service_metrics(
            project=project,
            granularity=granularity,
            metric_dates=list(computed_dates),
            set_baseline=set_baseline,
        )

        duration_ms = int((time.time() - start_time) * 1000)

        return {
            "status": "completed",
            "granularity": granularity,
            "computed_features": total_features,
            "computed_feature_views": total_views,
            "computed_feature_services": total_services,
            "metric_dates": sorted(d.isoformat() for d in computed_dates),
            "duration_ms": duration_ms,
        }

    # ------------------------------------------------------------------ #
    #  Transient compute (not stored)
    # ------------------------------------------------------------------ #

    def compute_transient(
        self,
        project: str,
        feature_view_name: str,
        feature_names: Optional[List[str]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """Compute metrics on-the-fly for an arbitrary date range without persisting."""
        start_time = time.time()
        start_dt, end_dt = self._to_date_range(start_date, end_date)
        effective_start = start_date or (date.today() - timedelta(days=1))
        effective_end = end_date or date.today()

        fv = self._store.registry.get_feature_view(
            name=feature_view_name, project=project
        )

        feature_fields = self._classify_fields(fv, feature_names=feature_names)
        if not feature_fields:
            return {
                "status": "completed",
                "feature_view_name": feature_view_name,
                "start_date": effective_start.isoformat(),
                "end_date": effective_end.isoformat(),
                "metrics": [],
                "duration_ms": int((time.time() - start_time) * 1000),
            }

        metrics_list = self._compute_feature_metrics(
            fv,
            feature_fields,
            start_dt,
            end_dt,
        )

        for m in metrics_list:
            m["feature_view_name"] = feature_view_name
            m["start_date"] = effective_start.isoformat()
            m["end_date"] = effective_end.isoformat()

        return {
            "status": "completed",
            "feature_view_name": feature_view_name,
            "start_date": effective_start.isoformat(),
            "end_date": effective_end.isoformat(),
            "metrics": metrics_list,
            "duration_ms": int((time.time() - start_time) * 1000),
        }

    # ------------------------------------------------------------------ #
    #  DQM Job helpers
    # ------------------------------------------------------------------ #

    def submit_job(
        self,
        project: str,
        job_type: str,
        feature_view_name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> str:
        return self.job_manager.submit(
            project=project,
            job_type=job_type,
            feature_view_name=feature_view_name,
            parameters=parameters,
        )

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        return self.job_manager.get_job(job_id)

    def execute_job(self, job_id: str) -> Dict[str, Any]:
        return self.job_manager.execute_job(job_id, self)

    # ------------------------------------------------------------------ #
    #  Read helpers (delegate to offline store)
    # ------------------------------------------------------------------ #

    def _query(
        self,
        metric_type: str,
        project: str,
        filters=None,
        start_date=None,
        end_date=None,
    ):
        self._ensure_monitoring_tables()
        return self._get_offline_store().query_monitoring_metrics(
            config=self._store.config,
            project=project,
            metric_type=metric_type,
            filters=filters,
            start_date=start_date,
            end_date=end_date,
        )

    def get_feature_metrics(
        self,
        project: str,
        feature_service_name: Optional[str] = None,
        feature_view_name: Optional[str] = None,
        feature_name: Optional[str] = None,
        granularity: Optional[str] = None,
        data_source_type: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        filters = {
            "feature_view_name": feature_view_name,
            "feature_name": feature_name,
            "granularity": granularity,
            "data_source_type": data_source_type,
        }
        if feature_service_name:
            return self._get_metrics_by_service(
                project,
                feature_service_name,
                lambda fv_name: self._query(
                    "feature",
                    project,
                    {**filters, "feature_view_name": fv_name},
                    start_date,
                    end_date,
                ),
            )
        return self._query("feature", project, filters, start_date, end_date)

    def get_feature_view_metrics(
        self,
        project: str,
        feature_service_name: Optional[str] = None,
        feature_view_name: Optional[str] = None,
        granularity: Optional[str] = None,
        data_source_type: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        filters = {
            "feature_view_name": feature_view_name,
            "granularity": granularity,
            "data_source_type": data_source_type,
        }
        if feature_service_name:
            return self._get_metrics_by_service(
                project,
                feature_service_name,
                lambda fv_name: self._query(
                    "feature_view",
                    project,
                    {**filters, "feature_view_name": fv_name},
                    start_date,
                    end_date,
                ),
            )
        return self._query("feature_view", project, filters, start_date, end_date)

    def get_feature_service_metrics(
        self,
        project: str,
        feature_service_name: Optional[str] = None,
        granularity: Optional[str] = None,
        data_source_type: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        filters = {
            "feature_service_name": feature_service_name,
            "granularity": granularity,
            "data_source_type": data_source_type,
        }
        return self._query("feature_service", project, filters, start_date, end_date)

    def get_baseline(
        self,
        project: str,
        feature_view_name: Optional[str] = None,
        feature_name: Optional[str] = None,
        data_source_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        filters = {
            "feature_view_name": feature_view_name,
            "feature_name": feature_name,
            "data_source_type": data_source_type,
            "is_baseline": True,
        }
        return self._query("feature", project, filters)

    def get_timeseries(
        self,
        project: str,
        feature_view_name: Optional[str] = None,
        feature_name: Optional[str] = None,
        feature_service_name: Optional[str] = None,
        granularity: Optional[str] = None,
        data_source_type: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        return self.get_feature_metrics(
            project=project,
            feature_service_name=feature_service_name,
            feature_view_name=feature_view_name,
            feature_name=feature_name,
            granularity=granularity,
            data_source_type=data_source_type,
            start_date=start_date,
            end_date=end_date,
        )

    # ------------------------------------------------------------------ #
    #  Auto-baseline trigger for feast apply
    # ------------------------------------------------------------------ #

    def submit_baseline_for_new_features(
        self,
        project: str,
        feature_views: Optional[List] = None,
    ) -> List[str]:
        """Submit baseline DQM jobs for feature views with new features.

        Called from feast apply. Returns list of submitted job IDs.
        Idempotent — only features without existing baselines are included.
        """
        if project is None:
            project = self._store.config.project

        if feature_views is None:
            feature_views = self._store.registry.list_feature_views(project=project)

        job_ids = []
        for fv in feature_views:
            new_features = self._get_features_without_baseline(project, fv)
            if not new_features:
                continue

            feature_names = [f.name for f in new_features]
            job_id = self.job_manager.submit(
                project=project,
                job_type="baseline",
                feature_view_name=fv.name,
                parameters={"feature_names": feature_names},
            )
            job_ids.append(job_id)
            logger.info(
                "Queued baseline computation for '%s' features %s (job: %s)",
                fv.name,
                feature_names,
                job_id,
            )

        return job_ids

    # ------------------------------------------------------------------ #
    #  Private: compute engine dispatch (SQL push-down → Python fallback)
    # ------------------------------------------------------------------ #

    def _compute_feature_metrics(
        self,
        feature_view,
        feature_fields: List[Tuple[str, str]],
        start_dt: datetime,
        end_dt: datetime,
    ) -> List[Dict[str, Any]]:
        """Compute metrics, preferring offline store SQL push-down.

        Falls back to Python-based (PyArrow/NumPy) computation when the
        offline store does not implement compute_monitoring_metrics.
        """
        provider = self._store._get_provider()
        offline_store = provider.offline_store
        try:
            return offline_store.compute_monitoring_metrics(
                config=self._store.config,
                data_source=feature_view.batch_source,
                feature_columns=feature_fields,
                timestamp_field=feature_view.batch_source.timestamp_field,
                start_date=start_dt,
                end_date=end_dt,
                histogram_bins=self._calculator.histogram_bins,
                top_n=self._calculator.top_n,
            )
        except NotImplementedError:
            logger.debug(
                "Offline store does not support compute_monitoring_metrics, "
                "falling back to Python-based computation"
            )
            arrow_table = self._read_batch_source(
                feature_view,
                feature_fields,
                start_dt,
                end_dt,
            )
            return self._calculator.compute_all(arrow_table, feature_fields)

    def _get_max_timestamp(self, feature_view) -> Optional[datetime]:
        """Query the batch source for MAX(event_timestamp).

        Prefers the offline store's native push-down; falls back to reading
        the full table and computing max in Python.
        """
        provider = self._store._get_provider()
        offline_store = provider.offline_store
        try:
            return offline_store.get_monitoring_max_timestamp(
                config=self._store.config,
                data_source=feature_view.batch_source,
                timestamp_field=feature_view.batch_source.timestamp_field,
            )
        except NotImplementedError:
            return self._get_max_timestamp_fallback(feature_view)

    def _get_max_timestamp_fallback(self, feature_view) -> Optional[datetime]:
        """Pull all data and compute max timestamp in Python (fallback)."""
        import pyarrow.compute as pc

        data_source = feature_view.batch_source
        ts_field = data_source.timestamp_field

        provider = self._store._get_provider()
        offline_store = provider.offline_store

        retrieval_job = offline_store.pull_all_from_table_or_query(
            config=self._store.config,
            data_source=data_source,
            join_key_columns=self._resolve_join_key_columns(feature_view),
            feature_name_columns=[],
            timestamp_field=ts_field,
            created_timestamp_column=data_source.created_timestamp_column,
            start_date=_EPOCH,
            end_date=_FAR_FUTURE,
        )

        table = retrieval_job.to_arrow()
        if ts_field not in table.column_names or len(table) == 0:
            return None

        max_val = pc.max(table.column(ts_field)).as_py()
        if max_val is None:
            return None

        if isinstance(max_val, datetime):
            return max_val if max_val.tzinfo else max_val.replace(tzinfo=timezone.utc)

        return datetime.combine(max_val, datetime.min.time(), tzinfo=timezone.utc)

    # ------------------------------------------------------------------ #
    #  Private: shared helpers (DRY)
    # ------------------------------------------------------------------ #

    @staticmethod
    def _to_date_range(
        start_date: Optional[date], end_date: Optional[date]
    ) -> Tuple[datetime, datetime]:
        today = date.today()
        if end_date is None:
            end_date = today
        if start_date is None:
            start_date = end_date - timedelta(days=1)
        start_dt = datetime(
            start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc
        )
        end_dt = datetime(
            end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc
        )
        return start_dt, end_dt

    @staticmethod
    def _classify_fields(
        feature_view,
        feature_names=None,
        fields=None,
    ) -> List[Tuple[str, str]]:
        """Extract and classify features as numeric/categorical.

        Args:
            feature_view: FeatureView to extract fields from (used if fields is None).
            feature_names: Optional filter list of feature names.
            fields: Optional pre-selected Field objects (e.g., from idempotency check).
        """
        if fields is None:
            fields = feature_view.features
        if feature_names:
            fields = [f for f in fields if f.name in feature_names]

        result = []
        for field in fields:
            ftype = MetricsCalculator.classify_feature(field.dtype)
            if ftype is None:
                logger.warning(
                    "Unsupported dtype '%s' for feature '%s', skipping",
                    field.dtype,
                    field.name,
                )
                continue
            result.append((field.name, ftype))
        return result

    def _save_computed_metrics(
        self,
        project: str,
        feature_view,
        metrics_list: List[Dict[str, Any]],
        metric_date: date,
        granularity: str,
        set_baseline: bool,
        now: datetime,
    ) -> None:
        if not metrics_list:
            return

        offline_store = self._get_offline_store()
        config = self._store.config

        if set_baseline:
            offline_store.clear_monitoring_baseline(
                config=config,
                project=project,
                feature_view_name=feature_view.name,
            )

        for m in metrics_list:
            m["project_id"] = project
            m["feature_view_name"] = feature_view.name
            m["metric_date"] = metric_date
            m["granularity"] = granularity
            m["data_source_type"] = "batch"
            m["computed_at"] = now
            m["is_baseline"] = set_baseline

        offline_store.save_monitoring_metrics(config, "feature", metrics_list)

        null_rates = [
            m["null_rate"] for m in metrics_list if m.get("null_rate") is not None
        ]
        view_metric = {
            "project_id": project,
            "feature_view_name": feature_view.name,
            "metric_date": metric_date,
            "granularity": granularity,
            "data_source_type": "batch",
            "computed_at": now,
            "is_baseline": set_baseline,
            "total_row_count": metrics_list[0]["row_count"] if metrics_list else 0,
            "total_features": len(metrics_list),
            "features_with_nulls": sum(
                1 for m in metrics_list if (m.get("null_count") or 0) > 0
            ),
            "avg_null_rate": sum(null_rates) / len(null_rates) if null_rates else 0.0,
            "max_null_rate": max(null_rates) if null_rates else 0.0,
        }
        offline_store.save_monitoring_metrics(config, "feature_view", [view_metric])

    def _resolve_join_key_columns(self, feature_view) -> List[str]:
        config = self._store.config
        return (
            [
                entity.name
                for entity in self._store.registry.list_entities(project=config.project)
                if entity.name in (feature_view.entities or [])
            ]
            or feature_view.entities
            or []
        )

    def _get_metrics_by_service(
        self, project: str, feature_service_name: str, query_fn
    ):
        fs = self._store.registry.get_feature_service(
            name=feature_service_name, project=project
        )
        fv_names = [proj.name for proj in fs.feature_view_projections]
        results = []
        for fv_name in fv_names:
            results.extend(query_fn(fv_name))
        return results

    def _resolve_feature_views(self, project: str, feature_view_name: Optional[str]):
        if feature_view_name:
            fv = self._store.registry.get_feature_view(
                name=feature_view_name, project=project
            )
            return [fv]
        return self._store.registry.list_feature_views(project=project)

    def _get_features_without_baseline(self, project, feature_view, feature_names=None):
        existing = self.get_baseline(
            project=project,
            feature_view_name=feature_view.name,
        )
        existing_names = {m["feature_name"] for m in existing}

        fields = feature_view.features
        if feature_names:
            fields = [f for f in fields if f.name in feature_names]

        return [f for f in fields if f.name not in existing_names]

    def _compute_for_feature_view(
        self,
        project: str,
        feature_view,
        feature_names: Optional[List[str]],
        start_dt: datetime,
        end_dt: datetime,
        granularity: str,
        set_baseline: bool,
    ) -> Dict[str, Any]:
        feature_fields = self._classify_fields(
            feature_view, feature_names=feature_names
        )
        if not feature_fields:
            return {"feature_count": 0, "dates": set()}

        metrics_list = self._compute_feature_metrics(
            feature_view,
            feature_fields,
            start_dt,
            end_dt,
        )

        now = datetime.now(timezone.utc)
        metric_date = start_dt.date()

        self._save_computed_metrics(
            project=project,
            feature_view=feature_view,
            metrics_list=metrics_list,
            metric_date=metric_date,
            granularity=granularity,
            set_baseline=set_baseline,
            now=now,
        )

        return {"feature_count": len(metrics_list), "dates": {metric_date}}

    # ------------------------------------------------------------------ #
    #  Private: log source helpers
    # ------------------------------------------------------------------ #

    def _resolve_log_source(self, feature_service):
        """Resolve log data source for a feature service.

        Returns (DataSource, timestamp_field, feature_fields, log_col_map)
        or None if the feature service has no logging configured.

        ``feature_fields`` uses the raw log column names (needed for
        SQL/PyArrow column access).  ``log_col_map`` maps each raw log
        column to ``(feature_view_name, normalized_feature_name)`` so
        callers can store metrics under the correct view and feature
        name — critical for drift detection across batch and log sources.
        """
        if not feature_service.logging_config:
            return None

        destination = feature_service.logging_config.destination
        try:
            data_source = destination.to_data_source()
        except NotImplementedError:
            logger.warning(
                "Logging destination for '%s' does not support to_data_source()",
                feature_service.name,
            )
            return None

        logging_source = FeatureServiceLoggingSource(
            feature_service,
            self._store.config.project,
        )
        schema = logging_source.get_schema(self._store.registry)

        skip_cols = {
            LOG_TIMESTAMP_FIELD,
            "__log_date",
            "__request_id",
        }
        entity_columns = set()
        view_feature_names: dict = {}
        for proj in feature_service.feature_view_projections:
            view_alias = proj.name_to_use()
            try:
                fv = self._store.registry.get_feature_view(
                    name=proj.name, project=self._store.config.project
                )
                for ec in fv.entity_columns:
                    entity_columns.add(ec.name)
            except Exception:
                pass
            for feat in proj.features:
                log_col = f"{view_alias}__{feat.name}"
                view_feature_names[log_col] = (proj.name, feat.name)

        feature_fields = []
        log_col_map: dict = {}
        for field in schema:
            if field.name in skip_cols or field.name in entity_columns:
                continue
            if field.name.endswith("__timestamp") or field.name.endswith("__status"):
                continue
            ftype = MetricsCalculator.classify_feature_arrow(field.type)
            if ftype is not None:
                feature_fields.append((field.name, ftype))
                if field.name in view_feature_names:
                    log_col_map[field.name] = view_feature_names[field.name]

        if not feature_fields:
            return None

        return data_source, LOG_TIMESTAMP_FIELD, feature_fields, log_col_map

    def _get_max_timestamp_for_source(self, data_source, ts_field):
        """Get MAX timestamp from an arbitrary data source."""
        provider = self._store._get_provider()
        offline_store = provider.offline_store
        try:
            return offline_store.get_monitoring_max_timestamp(
                config=self._store.config,
                data_source=data_source,
                timestamp_field=ts_field,
            )
        except NotImplementedError:
            return self._get_max_timestamp_for_source_fallback(
                data_source,
                ts_field,
            )

    def _get_max_timestamp_for_source_fallback(self, data_source, ts_field):
        """Pull data and compute max timestamp in Python (fallback)."""
        import pyarrow.compute as pc

        provider = self._store._get_provider()
        offline_store = provider.offline_store

        retrieval_job = offline_store.pull_all_from_table_or_query(
            config=self._store.config,
            data_source=data_source,
            join_key_columns=[],
            feature_name_columns=[],
            timestamp_field=ts_field,
            start_date=_EPOCH,
            end_date=_FAR_FUTURE,
        )

        table = retrieval_job.to_arrow()
        if ts_field not in table.column_names or len(table) == 0:
            return None

        max_val = pc.max(table.column(ts_field)).as_py()
        if max_val is None:
            return None

        if isinstance(max_val, datetime):
            return max_val if max_val.tzinfo else max_val.replace(tzinfo=timezone.utc)
        return datetime.combine(max_val, datetime.min.time(), tzinfo=timezone.utc)

    def _compute_from_source(
        self,
        data_source,
        ts_field: str,
        feature_fields: List[Tuple[str, str]],
        start_dt: datetime,
        end_dt: datetime,
    ) -> List[Dict[str, Any]]:
        """Compute metrics from an arbitrary data source (batch or log)."""
        provider = self._store._get_provider()
        offline_store = provider.offline_store
        try:
            return offline_store.compute_monitoring_metrics(
                config=self._store.config,
                data_source=data_source,
                feature_columns=feature_fields,
                timestamp_field=ts_field,
                start_date=start_dt,
                end_date=end_dt,
                histogram_bins=self._calculator.histogram_bins,
                top_n=self._calculator.top_n,
            )
        except NotImplementedError:
            logger.debug(
                "Offline store does not support compute_monitoring_metrics, "
                "falling back to Python-based computation for log source"
            )
            retrieval_job = offline_store.pull_all_from_table_or_query(
                config=self._store.config,
                data_source=data_source,
                join_key_columns=[],
                feature_name_columns=[name for name, _ in feature_fields],
                timestamp_field=ts_field,
                start_date=start_dt,
                end_date=end_dt,
            )
            arrow_table = retrieval_job.to_arrow()
            return self._calculator.compute_all(arrow_table, feature_fields)

    def _save_log_metrics(
        self,
        project: str,
        feature_service_name: str,
        log_col_map: Dict[str, Tuple[str, str]],
        metrics_list: List[Dict[str, Any]],
        metric_date: date,
        granularity: str,
        set_baseline: bool,
        now: datetime,
    ) -> None:
        """Save log-sourced metrics tagged with data_source_type='log'.

        Normalizes log column names (``driver_stats__conv_rate``) back to
        their originating ``feature_view_name`` and ``feature_name`` so
        that drift detection can join batch and log metrics on the same
        feature identity.
        """
        if not metrics_list:
            return

        offline_store = self._get_offline_store()
        config = self._store.config

        for m in metrics_list:
            log_col = m.get("feature_name", "")
            view_name, feat_name = log_col_map.get(
                log_col, (feature_service_name, log_col)
            )
            m["project_id"] = project
            m["feature_view_name"] = view_name
            m["feature_name"] = feat_name
            m["metric_date"] = metric_date
            m["granularity"] = granularity
            m["data_source_type"] = "log"
            m["computed_at"] = now
            m["is_baseline"] = set_baseline

        offline_store.save_monitoring_metrics(config, "feature", metrics_list)

        # --- per-feature-view aggregates (grouped by originating view) ---
        by_view: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for m in metrics_list:
            by_view[m["feature_view_name"]].append(m)

        view_metrics = []
        for vname, vmetrics in by_view.items():
            null_rates = [
                m["null_rate"] for m in vmetrics if m.get("null_rate") is not None
            ]
            view_metrics.append(
                {
                    "project_id": project,
                    "feature_view_name": vname,
                    "metric_date": metric_date,
                    "granularity": granularity,
                    "data_source_type": "log",
                    "computed_at": now,
                    "is_baseline": set_baseline,
                    "total_row_count": vmetrics[0]["row_count"] if vmetrics else 0,
                    "total_features": len(vmetrics),
                    "features_with_nulls": sum(
                        1 for m in vmetrics if (m.get("null_count") or 0) > 0
                    ),
                    "avg_null_rate": (
                        sum(null_rates) / len(null_rates) if null_rates else 0.0
                    ),
                    "max_null_rate": max(null_rates) if null_rates else 0.0,
                }
            )
        offline_store.save_monitoring_metrics(config, "feature_view", view_metrics)

        # --- feature service aggregate ---
        all_null_rates = [
            m["null_rate"] for m in metrics_list if m.get("null_rate") is not None
        ]
        svc_metric = {
            "project_id": project,
            "feature_service_name": feature_service_name,
            "metric_date": metric_date,
            "granularity": granularity,
            "data_source_type": "log",
            "computed_at": now,
            "is_baseline": set_baseline,
            "total_feature_views": len(by_view),
            "total_features": len(metrics_list),
            "avg_null_rate": (
                sum(all_null_rates) / len(all_null_rates) if all_null_rates else 0.0
            ),
            "max_null_rate": max(all_null_rates) if all_null_rates else 0.0,
        }
        offline_store.save_monitoring_metrics(config, "feature_service", [svc_metric])

    def _read_batch_source(self, feature_view, feature_fields, start_dt, end_dt):
        config = self._store.config
        data_source = feature_view.batch_source

        provider = self._store._get_provider()
        offline_store = provider.offline_store

        retrieval_job = offline_store.pull_all_from_table_or_query(
            config=config,
            data_source=data_source,
            join_key_columns=self._resolve_join_key_columns(feature_view),
            feature_name_columns=[name for name, _ in feature_fields],
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            start_date=start_dt,
            end_date=end_dt,
        )

        return retrieval_job.to_arrow()

    def _compute_feature_service_metrics(
        self,
        project: str,
        granularity: str,
        metric_dates: List[date],
        set_baseline: bool,
    ) -> int:
        if not metric_dates:
            return 0

        feature_services = self._store.registry.list_feature_services(project=project)
        if not feature_services:
            return 0

        offline_store = self._get_offline_store()
        config = self._store.config
        now = datetime.now(timezone.utc)
        count = 0

        for fs in feature_services:
            try:
                fv_names = [proj.name for proj in fs.feature_view_projections]

                for metric_date in metric_dates:
                    fv_metrics = offline_store.query_monitoring_metrics(
                        config=config,
                        project=project,
                        metric_type="feature_view",
                        filters={
                            "granularity": granularity,
                            "data_source_type": "batch",
                        },
                        start_date=metric_date,
                        end_date=metric_date,
                    )

                    relevant = [
                        m for m in fv_metrics if m.get("feature_view_name") in fv_names
                    ]
                    if not relevant:
                        continue

                    null_rates = [
                        m["avg_null_rate"]
                        for m in relevant
                        if m.get("avg_null_rate") is not None
                    ]

                    service_metric = {
                        "project_id": project,
                        "feature_service_name": fs.name,
                        "metric_date": metric_date
                        if isinstance(metric_date, date)
                        else date.fromisoformat(str(metric_date)),
                        "granularity": granularity,
                        "data_source_type": "batch",
                        "computed_at": now,
                        "is_baseline": set_baseline,
                        "total_feature_views": len(relevant),
                        "total_features": sum(
                            m.get("total_features", 0) for m in relevant
                        ),
                        "avg_null_rate": (
                            sum(null_rates) / len(null_rates) if null_rates else 0.0
                        ),
                        "max_null_rate": max(null_rates) if null_rates else 0.0,
                    }
                    offline_store.save_monitoring_metrics(
                        config,
                        "feature_service",
                        [service_metric],
                    )
                    count += 1
            except Exception:
                logger.exception("Failed to compute service metrics for '%s'", fs.name)

        return count
