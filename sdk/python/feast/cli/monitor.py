from datetime import date
from typing import List, Optional

import click

from feast.infra.offline_stores.offline_store import OfflineStore
from feast.repo_operations import create_feature_store

VALID_GRANULARITIES = OfflineStore.MONITORING_VALID_GRANULARITIES


@click.group(name="monitor")
def monitor_cmd():
    """Feature monitoring commands."""
    pass


@monitor_cmd.command("run")
@click.option(
    "--project",
    "-p",
    default=None,
    help="Feast project name. Defaults to the project in feature_store.yaml.",
)
@click.option(
    "--feature-view",
    "-v",
    default=None,
    help="Feature view name. If omitted, all feature views are computed.",
)
@click.option(
    "--feature-name",
    "-f",
    multiple=True,
    help="Feature name(s) to compute. Can be specified multiple times.",
)
@click.option(
    "--start-date",
    default=None,
    help="Start date (YYYY-MM-DD). If omitted, auto-detected from source data.",
)
@click.option(
    "--end-date",
    default=None,
    help="End date (YYYY-MM-DD). If omitted, auto-detected from source data.",
)
@click.option(
    "--granularity",
    "-g",
    default=None,
    type=click.Choice(list(VALID_GRANULARITIES)),
    help="Metric granularity. If omitted, all granularities are computed (auto mode).",
)
@click.option(
    "--set-baseline",
    is_flag=True,
    default=False,
    help="Mark this computation as the baseline for drift detection.",
)
@click.pass_context
def monitor_run(
    ctx: click.Context,
    project: Optional[str],
    feature_view: Optional[str],
    feature_name: tuple,
    start_date: Optional[str],
    end_date: Optional[str],
    granularity: Optional[str],
    set_baseline: bool,
):
    """Compute feature quality metrics.

    Without --start-date/--end-date/--granularity, runs in auto mode:
    detects date ranges from source data and computes all granularities.
    """
    store = create_feature_store(ctx)

    if project is None:
        project = store.project

    from feast.monitoring.monitoring_service import MonitoringService

    svc = MonitoringService(store)

    auto_mode = start_date is None and end_date is None and granularity is None
    feat_names: Optional[List[str]] = list(feature_name) if feature_name else None

    if auto_mode and not set_baseline:
        click.echo("Auto-computing metrics for all granularities...")
        result = svc.auto_compute(
            project=project,
            feature_view_name=feature_view,
        )
        click.echo(f"Status: {result['status']}")
        click.echo(f"Feature views computed: {result['computed_feature_views']}")
        click.echo(f"Features computed: {result['computed_features']}")
        click.echo(f"Granularities: {', '.join(result['granularities'])}")
        click.echo(f"Duration: {result['duration_ms']}ms")
    else:
        start_d = date.fromisoformat(start_date) if start_date else None
        end_d = date.fromisoformat(end_date) if end_date else None

        result = svc.compute_metrics(
            project=project,
            feature_view_name=feature_view,
            feature_names=feat_names,
            start_date=start_d,
            end_date=end_d,
            granularity=granularity or "daily",
            set_baseline=set_baseline,
        )

        click.echo(f"Status: {result['status']}")
        click.echo(f"Granularity: {result['granularity']}")
        click.echo(f"Features computed: {result['computed_features']}")
        click.echo(f"Feature views computed: {result['computed_feature_views']}")
        click.echo(f"Feature services computed: {result['computed_feature_services']}")
        click.echo(f"Metric dates: {', '.join(result['metric_dates'])}")
        click.echo(f"Duration: {result['duration_ms']}ms")

        if set_baseline:
            click.echo("Baseline: SET")
