"""CLI commands for MLflow GenAI Dataset sync utilities."""

import json
import os

import click


@click.group(name="datasets")
def datasets_cmd():
    """MLflow GenAI Dataset sync utilities."""
    pass


@datasets_cmd.command("sync")
@click.option(
    "--source",
    required=True,
    help="MLflow GenAI dataset name.",
)
@click.option(
    "--feature-view",
    required=True,
    help="Target Feast FeatureView name to ingest into.",
)
@click.option(
    "--full-refresh/--incremental",
    default=False,
    help="Full refresh re-syncs all records; incremental only syncs new/updated.",
)
@click.option(
    "--field-mapping",
    default=None,
    help="Path to a JSON file with field mapping overrides.",
)
@click.option(
    "--batch-size",
    default=10_000,
    type=int,
    help="Number of rows to write per batch (default: 10000).",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    help="Fetch and flatten without writing to stores.",
)
@click.pass_context
def sync_cmd(
    ctx, source, feature_view, full_refresh, field_mapping, batch_size, dry_run
):
    """Sync an MLflow GenAI Dataset into a Feast FeatureView."""
    from feast.mlflow_integration.dataset_sync import sync_mlflow_dataset_to_feast
    from feast.repo_operations import create_feature_store

    store = create_feature_store(ctx)
    tracking_uri = _resolve_tracking_uri(store)

    mapping = None
    if field_mapping:
        with open(field_mapping) as f:
            mapping = json.load(f)

    click.echo(
        f"Syncing MLflow dataset '{source}' → FeatureView '{feature_view}' "
        f"({'full refresh' if full_refresh else 'incremental'})"
    )
    if tracking_uri:
        click.echo(f"  MLflow tracking URI: {tracking_uri}")
    if dry_run:
        click.echo("  DRY RUN — no data will be written.")

    result = sync_mlflow_dataset_to_feast(
        store=store,
        dataset_name=source,
        feature_view_name=feature_view,
        field_mapping=mapping,
        tracking_uri=tracking_uri,
        incremental=not full_refresh,
        batch_size=batch_size,
        dry_run=dry_run,
    )

    click.echo("\nSync results:")
    click.echo(f"  Records fetched:  {result.records_fetched}")
    click.echo(f"  Records ingested: {result.records_ingested}")
    click.echo(f"  New records:      {result.new_records}")

    if result.errors:
        click.echo(f"\n  Errors ({len(result.errors)}):")
        for err in result.errors:
            click.echo(f"    - {err}", err=True)
        raise SystemExit(1)


@datasets_cmd.command("preview")
@click.option(
    "--source",
    required=True,
    help="MLflow GenAI dataset name.",
)
@click.option(
    "--limit",
    default=5,
    type=int,
    help="Number of records to preview (default: 5).",
)
@click.option(
    "--field-mapping",
    default=None,
    help="Path to a JSON file with field mapping overrides.",
)
@click.pass_context
def preview_cmd(ctx, source, limit, field_mapping):
    """Preview flattened records from an MLflow GenAI Dataset."""
    from feast.mlflow_integration.dataset_sync import (
        _fetch_dataset_with_retry,
        flatten_mlflow_dataset_df,
    )
    from feast.repo_operations import create_feature_store

    store = create_feature_store(ctx)
    tracking_uri = _resolve_tracking_uri(store)

    if tracking_uri:
        import mlflow

        mlflow.set_tracking_uri(tracking_uri)

    mapping = None
    if field_mapping:
        with open(field_mapping) as f:
            mapping = json.load(f)

    click.echo(f"Fetching dataset '{source}'...")
    dataset = _fetch_dataset_with_retry(source)
    if dataset is None:
        raise click.ClickException(f"Failed to fetch MLflow dataset '{source}'.")

    df = dataset.to_df()
    click.echo(f"  Total records: {len(df)}")

    if df.empty:
        click.echo("  Dataset is empty.")
        return

    df = flatten_mlflow_dataset_df(df, field_mapping=mapping)
    preview = df.head(limit)

    click.echo(f"\nFlattened preview ({min(limit, len(df))} rows):")
    click.echo(preview.to_string(index=False))


@datasets_cmd.command("sync-assessments")
@click.option(
    "--experiment",
    required=True,
    help="MLflow experiment name to scan for traces with assessments.",
)
@click.option(
    "--feature-view",
    required=True,
    help="Target Feast FeatureView/LabelView name to ingest into.",
)
@click.option(
    "--filter",
    "filter_str",
    default=None,
    help="MLflow search_traces filter string.",
)
@click.option(
    "--max-results",
    default=1000,
    type=int,
    help="Maximum number of traces to scan (default: 1000).",
)
@click.option(
    "--assessment-names",
    default=None,
    help="Comma-separated assessment names to sync (default: all).",
)
@click.option(
    "--batch-size",
    default=10_000,
    type=int,
    help="Number of rows to write per batch (default: 10000).",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    help="Extract without writing to stores.",
)
@click.option(
    "--pivot/--no-pivot",
    default=False,
    help="Pivot assessments into LabelView-compatible rows (one row per trace_id).",
)
@click.option(
    "--assessment-mapping",
    default=None,
    help="Path to a JSON file mapping assessment names to LabelView column names.",
)
@click.option(
    "--labeler-column",
    default="labeler",
    help="Target column name for assessment source_id when --pivot is used.",
)
@click.pass_context
def sync_assessments_cmd(
    ctx,
    experiment,
    feature_view,
    filter_str,
    max_results,
    assessment_names,
    batch_size,
    dry_run,
    pivot,
    assessment_mapping,
    labeler_column,
):
    """Sync assessments (expectations + feedback) from MLflow traces into Feast."""
    from feast.mlflow_integration.dataset_sync import sync_trace_assessments_to_feast
    from feast.repo_operations import create_feature_store

    store = create_feature_store(ctx)
    tracking_uri = _resolve_tracking_uri(store)

    if tracking_uri:
        click.echo(f"  MLflow tracking URI: {tracking_uri}")

    names_list = None
    if assessment_names:
        names_list = [n.strip() for n in assessment_names.split(",") if n.strip()]

    mapping = None
    if assessment_mapping:
        with open(assessment_mapping) as f:
            mapping = json.load(f)

    click.echo(
        f"Syncing assessments from experiment '{experiment}' → "
        f"{'LabelView' if pivot else 'FeatureView'} '{feature_view}'"
    )
    if pivot:
        click.echo("  Pivot mode: one row per trace_id (LabelView-compatible)")
    if dry_run:
        click.echo("  DRY RUN — no data will be written.")

    result = sync_trace_assessments_to_feast(
        store=store,
        experiment_name=experiment,
        feature_view_name=feature_view,
        tracking_uri=tracking_uri,
        filter_string=filter_str,
        max_results=max_results,
        assessment_names=names_list,
        batch_size=batch_size,
        dry_run=dry_run,
        pivot=pivot,
        assessment_mapping=mapping,
        labeler_column=labeler_column,
    )

    click.echo("\nSync results:")
    click.echo(f"  Assessments found:  {result.records_fetched}")
    click.echo(f"  Records ingested:   {result.records_ingested}")

    if result.errors:
        click.echo(f"\n  Errors ({len(result.errors)}):")
        for err in result.errors:
            click.echo(f"    - {err}", err=True)
        raise SystemExit(1)


def _resolve_tracking_uri(store) -> str | None:
    """Read the MLflow tracking URI from store config or environment."""
    mlflow_cfg = getattr(store.config, "mlflow", None)
    if mlflow_cfg is not None:
        uri = getattr(mlflow_cfg, "tracking_uri", None)
        if uri:
            return uri
    return os.environ.get("MLFLOW_TRACKING_URI")
