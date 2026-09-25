"""CLI commands for the Feast–MLflow DataSource integration."""

from __future__ import annotations

import json

import click


@click.group(name="mlflow")
def mlflow_cmd():
    """MLflow integration utilities (dataset sync, source validation)."""
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
    from feast.infra.data_sources.mlflow.auth import (
        mlflow_request_scope,
        resolve_mlflow_token,
    )
    from feast.mlflow_integration.dataset_sync import (
        _fetch_dataset_with_retry,
        flatten_mlflow_dataset_df,
    )
    from feast.repo_operations import create_feature_store

    store = create_feature_store(ctx)
    tracking_uri = _resolve_tracking_uri(store)

    mapping = None
    if field_mapping:
        with open(field_mapping) as f:
            mapping = json.load(f)

    token = resolve_mlflow_token()

    click.echo(f"Fetching dataset '{source}'...")
    with mlflow_request_scope(token, tracking_uri):
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


@mlflow_cmd.command("validate-source")
@click.argument("feature_view_name")
@click.pass_context
def validate_source_cmd(ctx, feature_view_name):
    """Validate that an MlflowDatasetSource is reachable and schema-compatible.

    Checks that the MLflow artifact or GenAI dataset referenced by the
    FeatureView's source exists, has a supported format, and that its
    schema matches the FeatureView's expected features.
    """
    _require_mlflow()
    from feast.infra.data_sources.mlflow.mlflow_dataset_source import (
        MlflowDatasetSource,
    )
    from feast.repo_operations import create_feature_store

    store = create_feature_store(ctx)

    try:
        fv = store.get_feature_view(feature_view_name)
    except Exception as e:
        raise click.ClickException(f"FeatureView '{feature_view_name}' not found: {e}")

    source = fv.batch_source
    if not isinstance(source, MlflowDatasetSource):
        raise click.ClickException(
            f"FeatureView '{feature_view_name}' is not backed by an "
            f"MlflowDatasetSource (source type: {type(source).__name__})"
        )

    click.echo(f"Validating MlflowDatasetSource for '{feature_view_name}'...")

    if source.is_genai_mode:
        click.echo(
            f"  Mode: GenAI Dataset "
            f"(name={source.dataset_name}, id={source.dataset_id})"
        )
    else:
        click.echo(
            f"  Mode: Artifact "
            f"(run_id={source.run_id}, path={source.artifact_path}, "
            f"format={source.artifact_format})"
        )

    tracking_uri = _resolve_tracking_uri(store)
    if tracking_uri:
        click.echo(f"  Tracking URI: {tracking_uri}")

    errors = []
    cols = []

    try:
        source.validate(store.config)
        click.echo("  Config validation: PASS")
    except Exception as e:
        errors.append(f"Config validation: {e}")
        click.echo(f"  Config validation: FAIL — {e}")

    try:
        cols = list(source.get_table_column_names_and_types(store.config))
        click.echo(f"  Schema introspection: PASS ({len(cols)} columns)")
        for col_name, col_type in cols:
            click.echo(f"    {col_name}: {col_type}")
    except Exception as e:
        errors.append(f"Schema introspection: {e}")
        click.echo(f"  Schema introspection: FAIL — {e}")

    if cols:
        fv_features = [f.name for f in fv.features]
        source_cols = {c[0] for c in cols}
        missing = [f for f in fv_features if f not in source_cols]
        if missing:
            errors.append(f"Missing columns in source: {missing}")
            click.echo(f"  Schema match: FAIL — missing columns: {missing}")
        else:
            click.echo("  Schema match: PASS")

    if errors:
        click.echo(f"\nValidation FAILED with {len(errors)} error(s).")
        raise SystemExit(1)
    else:
        click.echo("\nValidation PASSED.")


@mlflow_cmd.command("list-sources")
@click.pass_context
def list_sources_cmd(ctx):
    """List all FeatureViews backed by MlflowDatasetSource."""
    from feast.repo_operations import create_feature_store

    store = create_feature_store(ctx)

    try:
        from feast.infra.data_sources.mlflow.mlflow_dataset_source import (
            MlflowDatasetSource,
        )
    except ImportError:
        raise click.ClickException(
            "MlflowDatasetSource is not available. "
            "Install feast[mlflow] to use this command."
        )

    feature_views = store.list_feature_views()
    mlflow_views = []

    for fv in feature_views:
        if isinstance(fv.batch_source, MlflowDatasetSource):
            mlflow_views.append(fv)

    if not mlflow_views:
        click.echo("No FeatureViews backed by MlflowDatasetSource found.")
        return

    click.echo(
        f"Found {len(mlflow_views)} MlflowDatasetSource-backed FeatureView(s):\n"
    )
    for fv in mlflow_views:
        src = fv.batch_source
        assert isinstance(src, MlflowDatasetSource)
        click.echo(f"  {fv.name}:")
        if src.is_genai_mode:
            click.echo(
                f"    Mode: GenAI Dataset "
                f"(name={src.dataset_name}, id={src.dataset_id})"
            )
        else:
            click.echo(
                f"    Mode: Artifact "
                f"(run_id={src.run_id}, path={src.artifact_path}, "
                f"format={src.artifact_format})"
            )
        if src.tracking_uri:
            click.echo(f"    Tracking URI: {src.tracking_uri}")
        click.echo(f"    Batch source: {type(src.batch_source).__name__}")
        click.echo()
