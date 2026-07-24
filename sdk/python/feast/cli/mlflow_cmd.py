"""CLI commands for the Feast–MLflow integration."""

from __future__ import annotations

import json
from typing import Literal

import click


@click.group(name="mlflow")
def mlflow_cmd():
    """MLflow integration utilities (dataset sync, trace export)."""
    pass


def _require_mlflow() -> None:
    try:
        import mlflow  # noqa: F401
    except ImportError as e:
        raise click.ClickException(
            "The 'mlflow' package is required. Install it with: pip install 'feast[mlflow]'"
        ) from e


def _resolve_tracking_uri(store) -> str | None:
    """Read the MLflow tracking URI from store config or environment."""
    from feast.mlflow_integration.config import resolve_tracking_uri

    mlflow_cfg = getattr(store.config, "mlflow", None)
    if mlflow_cfg is not None and hasattr(mlflow_cfg, "get_tracking_uri"):
        return mlflow_cfg.get_tracking_uri()
    return resolve_tracking_uri(None)


def _require_tracking_uri(store) -> str:
    uri = _resolve_tracking_uri(store)
    if uri:
        return uri
    raise click.UsageError(
        "No MLflow tracking URI found. Set 'mlflow.tracking_uri' in "
        "feature_store.yaml or the MLFLOW_TRACKING_URI environment variable."
    )


def _parse_tags(raw: str | None) -> dict[str, str] | None:
    """Parse ``key=value,key2=value2`` into a dict."""
    if not raw:
        return None
    tags: dict[str, str] = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if "=" in pair:
            k, v = pair.split("=", 1)
            tags[k.strip()] = v.strip()
    return tags or None


def _print_sync_result(result) -> None:
    click.echo("\nSync results:")
    click.echo(f"  Records fetched:  {result.records_fetched}")
    click.echo(f"  Records ingested: {result.records_ingested}")
    click.echo(f"  New records:      {result.new_records}")
    if result.errors:
        click.echo(f"\n  Errors ({len(result.errors)}):")
        for err in result.errors:
            click.echo(f"    - {err}", err=True)
        raise SystemExit(1)


@mlflow_cmd.command("export-traces")
@click.option(
    "--experiment",
    required=True,
    help="MLflow experiment name to extract traces from.",
)
@click.option(
    "--output",
    "-o",
    required=True,
    help="Output JSONL file path.",
)
@click.option(
    "--format",
    "fmt",
    default="openai",
    type=click.Choice(["openai", "enriched"]),
    help="Export format (default: openai).",
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
    help="Maximum number of traces to process.",
)
@click.option(
    "--labeled-only/--all-traces",
    default=True,
    help="Only export traces that have labels (default: labeled-only).",
)
@click.option(
    "--dataset",
    default=None,
    help="MLflow dataset name — only export traces in this curated dataset.",
)
@click.option(
    "--register/--no-register",
    default=False,
    help="Save the JSONL as an MLflow artifact.",
)
@click.option(
    "--register-experiment",
    default=None,
    help="MLflow experiment to save the artifact in (default: same as --experiment).",
)
@click.option(
    "--register-run",
    default=None,
    help="Existing MLflow run ID to attach the artifact to.",
)
@click.option(
    "--dataset-tags",
    default=None,
    help="Comma-separated key=value tags for the MLflow run.",
)
@click.option(
    "--label-view",
    default=None,
    help="Feast LabelView name to join labels from (preferred over MLflow expectations).",
)
@click.option(
    "--label-fields",
    default="corrected_response",
    help="Comma-separated LabelView fields to fetch (default: corrected_response).",
)
@click.option(
    "--label-join-key",
    default="trace_id",
    help="Entity column used to join LabelView rows to traces (default: trace_id).",
)
@click.option(
    "--label-source",
    default="historical",
    type=click.Choice(["historical", "online"]),
    help=(
        "How to resolve LabelView labels: 'historical' applies ConflictPolicy "
        "via offline pull + resolve_conflicts (default); 'online' uses "
        "get_online_features (LAST_WRITE_WINS)."
    ),
)
@click.pass_context
def export_traces_cmd(
    ctx: click.Context,
    experiment: str,
    output: str,
    fmt: str,
    filter_str: str,
    max_results: int,
    labeled_only: bool,
    dataset: str,
    register: bool,
    register_experiment: str,
    register_run: str,
    dataset_tags: str,
    label_view: str,
    label_fields: str,
    label_join_key: str,
    label_source: Literal["historical", "online"],
):
    """Export fine-tuning JSONL from MLflow traces with labels."""
    _require_mlflow()
    from feast.finetuning.exporters import get_exporter
    from feast.finetuning.label_resolver import (
        filter_labeled_only,
        resolve_labels_from_feast,
        resolve_labels_from_mlflow,
    )
    from feast.finetuning.trace_extractor import extract_from_traces
    from feast.repo_operations import create_feature_store

    store = create_feature_store(ctx)
    tracking_uri = _require_tracking_uri(store)

    click.echo(
        f"Extracting traces from experiment '{experiment}' (max {max_results})..."
    )
    examples = extract_from_traces(
        tracking_uri=tracking_uri,
        experiment_name=experiment,
        filter_string=filter_str,
        max_results=max_results,
    )
    click.echo(f"  Found {len(examples)} trace(s) with CHAT_MODEL spans.")

    if not examples:
        click.echo("No traces to export.")
        return

    if dataset:
        from feast.finetuning.dataset_filter import filter_by_mlflow_dataset

        examples = filter_by_mlflow_dataset(
            examples, dataset_name=dataset, tracking_uri=tracking_uri
        )
        click.echo(f"  Filtered to {len(examples)} trace(s) in dataset '{dataset}'.")
        if not examples:
            click.echo("No traces in dataset to export.")
            return

    if label_view:
        fields = [f.strip() for f in label_fields.split(",") if f.strip()]
        click.echo(
            f"  Resolving labels from Feast LabelView '{label_view}' "
            f"(fields={fields}, join_key={label_join_key}, source={label_source})..."
        )
        examples = resolve_labels_from_feast(
            examples,
            store=store,
            label_view_name=label_view,
            label_fields=fields,
            join_key=label_join_key,
            label_source=label_source,
        )
    else:
        click.echo("  Resolving labels from MLflow expectations & feedback...")
        examples = resolve_labels_from_mlflow(examples)

    labeled_count = sum(1 for ex in examples if ex.corrected_response is not None)
    click.echo(f"  {labeled_count}/{len(examples)} trace(s) have labels.")

    if labeled_only:
        examples = filter_labeled_only(examples)
        if not examples:
            click.echo(
                "No labeled traces to export (use --all-traces to include unlabeled)."
            )
            return

    exporter = get_exporter(fmt)
    count = exporter.export(examples, output)
    click.echo(f"Exported {count} example(s) to {output}")

    if register:
        tags_dict = _parse_tags(dataset_tags)
        run_id = exporter.register_in_mlflow(
            output_path=output,
            experiment_name=register_experiment or experiment,
            run_id=register_run,
            context="fine-tuning",
            tags=tags_dict,
        )
        if run_id:
            click.echo(f"Registered artifact in MLflow (run_id={run_id})")
        else:
            click.echo("Warning: failed to register artifact in MLflow.", err=True)


@mlflow_cmd.command("sync-dataset")
@click.option(
    "--feature-view",
    default=None,
    help=(
        "Target FeatureView/LabelView. When omitted, syncs all views whose "
        "source is MlflowDatasetSource."
    ),
)
@click.option(
    "--source",
    default=None,
    help="MLflow GenAI dataset name override (defaults to MlflowDatasetSource).",
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
    default=None,
    type=int,
    help="Number of rows to write per batch.",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    help="Fetch and flatten without writing to stores.",
)
@click.pass_context
def sync_dataset_cmd(
    ctx, feature_view, source, full_refresh, field_mapping, batch_size, dry_run
):
    """Sync an MLflow GenAI Dataset into a Feast FeatureView/LabelView."""
    _require_mlflow()
    from feast.mlflow_integration.dataset_sync import (
        sync_all_mlflow_dataset_sources,
        sync_mlflow_dataset_to_feast,
    )
    from feast.repo_operations import create_feature_store

    store = create_feature_store(ctx)
    tracking_uri = _resolve_tracking_uri(store)

    mapping = None
    if field_mapping:
        with open(field_mapping) as f:
            mapping = json.load(f)

    if tracking_uri:
        click.echo(f"  MLflow tracking URI: {tracking_uri}")
    if dry_run:
        click.echo("  DRY RUN — no data will be written.")

    if feature_view is None:
        click.echo(
            f"Syncing all MlflowDatasetSource views "
            f"({'full refresh' if full_refresh else 'incremental'})"
        )
        results = sync_all_mlflow_dataset_sources(
            store,
            incremental=not full_refresh,
            batch_size=batch_size,
            dry_run=dry_run,
        )
        if not results:
            click.echo("No FeatureViews/LabelViews with MlflowDatasetSource found.")
            return
        any_errors = False
        for name, result in results.items():
            click.echo(f"\n[{name}]")
            click.echo(f"  Records fetched:  {result.records_fetched}")
            click.echo(f"  Records ingested: {result.records_ingested}")
            if result.errors:
                any_errors = True
                for err in result.errors:
                    click.echo(f"    - {err}", err=True)
        if any_errors:
            raise SystemExit(1)
        return

    click.echo(
        f"Syncing MLflow dataset → FeatureView '{feature_view}' "
        f"({'full refresh' if full_refresh else 'incremental'})"
    )
    result = sync_mlflow_dataset_to_feast(
        store=store,
        feature_view_name=feature_view,
        dataset_name=source,
        field_mapping=mapping,
        tracking_uri=tracking_uri,
        incremental=not full_refresh,
        batch_size=batch_size,
        dry_run=dry_run,
    )
    _print_sync_result(result)


@mlflow_cmd.command("preview-dataset")
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
def preview_dataset_cmd(ctx, source, limit, field_mapping):
    """Preview flattened records from an MLflow GenAI Dataset."""
    _require_mlflow()
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


@mlflow_cmd.command("sync-assessments")
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
    default=None,
    type=int,
    help="Number of rows to write per batch.",
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
    _require_mlflow()
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
