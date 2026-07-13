"""CLI commands for fine-tuning dataset export."""

import click


@click.group(name="finetuning")
def finetuning_cmd():
    """Fine-tuning dataset utilities."""
    pass


@finetuning_cmd.command("export")
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
    help="MLflow dataset name — only export traces that belong to this curated dataset.",
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
    help="Existing MLflow run ID to attach the artifact to. Creates a new run if omitted.",
)
@click.option(
    "--dataset-tags",
    default=None,
    help="Comma-separated key=value tags for the MLflow run (e.g. 'use_case=red-teaming').",
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
@click.pass_context
def export_cmd(
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
):
    """Export fine-tuning JSONL from MLflow traces with labels."""
    from feast.finetuning.exporters import get_exporter
    from feast.finetuning.label_resolver import (
        filter_labeled_only,
        resolve_labels_from_feast,
        resolve_labels_from_mlflow,
    )
    from feast.finetuning.trace_extractor import extract_from_traces
    from feast.repo_operations import create_feature_store

    store = create_feature_store(ctx)

    tracking_uri = _resolve_tracking_uri(store)

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
            f"(fields={fields}, join_key={label_join_key})..."
        )
        examples = resolve_labels_from_feast(
            examples,
            store=store,
            label_view_name=label_view,
            label_fields=fields,
            join_key=label_join_key,
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


def _resolve_tracking_uri(store) -> str:  # type: ignore[no-untyped-def]
    """Read the MLflow tracking URI from store config or environment."""
    from feast.mlflow_integration.config import resolve_tracking_uri

    mlflow_cfg = getattr(store.config, "mlflow", None)
    if mlflow_cfg is not None and hasattr(mlflow_cfg, "get_tracking_uri"):
        uri = mlflow_cfg.get_tracking_uri()
    else:
        uri = resolve_tracking_uri(None)

    if uri:
        return uri

    raise click.UsageError(
        "No MLflow tracking URI found. Set 'mlflow.tracking_uri' in "
        "feature_store.yaml or the MLFLOW_TRACKING_URI environment variable."
    )
