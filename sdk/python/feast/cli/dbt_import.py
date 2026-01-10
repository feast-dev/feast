"""
CLI commands for importing dbt models as Feast features.

This module provides the `feast dbt` command group for integrating
dbt models with Feast feature stores.
"""

from typing import Any, Dict, List, Optional

import click
from colorama import Fore, Style

from feast.repo_operations import cli_check_repo, create_feature_store


@click.group(name="dbt")
def dbt_cmd():
    """Import dbt models as Feast features."""
    pass


@dbt_cmd.command("import")
@click.option(
    "--manifest-path",
    "-m",
    required=True,
    type=click.Path(exists=True),
    help="Path to dbt manifest.json file (typically target/manifest.json)",
)
@click.option(
    "--entity-column",
    "-e",
    required=True,
    help="Primary key / entity column name (e.g., driver_id, customer_id)",
)
@click.option(
    "--data-source-type",
    "-d",
    type=click.Choice(["bigquery", "snowflake", "file"]),
    default="bigquery",
    show_default=True,
    help="Type of data source to create",
)
@click.option(
    "--timestamp-field",
    "-t",
    default="event_timestamp",
    show_default=True,
    help="Timestamp field name for point-in-time joins",
)
@click.option(
    "--tag",
    "tag_filter",
    default=None,
    help="Only import models with this dbt tag (e.g., --tag feast)",
)
@click.option(
    "--model",
    "model_names",
    multiple=True,
    help="Specific model names to import (can be specified multiple times)",
)
@click.option(
    "--ttl-days",
    type=int,
    default=1,
    show_default=True,
    help="TTL (time-to-live) in days for feature views",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Preview what would be created without applying changes",
)
@click.option(
    "--exclude-columns",
    default=None,
    help="Comma-separated list of columns to exclude from features",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    help="Output Python file path (e.g., features.py). Generates code instead of applying to registry.",
)
@click.pass_context
def import_command(
    ctx: click.Context,
    manifest_path: str,
    entity_column: str,
    data_source_type: str,
    timestamp_field: str,
    tag_filter: Optional[str],
    model_names: tuple,
    ttl_days: int,
    dry_run: bool,
    exclude_columns: Optional[str],
    output: Optional[str],
):
    """
    Import dbt models as Feast FeatureViews.

    This command parses a dbt manifest.json file and creates corresponding
    Feast DataSource and FeatureView objects.

    Examples:

        # Import all models with 'feast' tag
        feast dbt import -m target/manifest.json -e driver_id --tag feast

        # Import specific models
        feast dbt import -m target/manifest.json -e customer_id --model orders --model customers

        # Dry run to preview changes
        feast dbt import -m target/manifest.json -e driver_id --tag feast --dry-run

        # Generate Python file instead of applying to registry
        feast dbt import -m target/manifest.json -e driver_id --tag feast --output features.py
    """
    from feast.dbt.mapper import DbtToFeastMapper
    from feast.dbt.parser import DbtManifestParser

    # Parse manifest
    click.echo(f"{Fore.CYAN}Parsing dbt manifest: {manifest_path}{Style.RESET_ALL}")

    try:
        parser = DbtManifestParser(manifest_path)
        parser.parse()
    except FileNotFoundError as e:
        click.echo(f"{Fore.RED}Error: {e}{Style.RESET_ALL}", err=True)
        raise SystemExit(1)
    except ValueError as e:
        click.echo(f"{Fore.RED}Error: {e}{Style.RESET_ALL}", err=True)
        raise SystemExit(1)

    # Display manifest info
    if parser.dbt_version:
        click.echo(f"  dbt version: {parser.dbt_version}")
    if parser.project_name:
        click.echo(f"  Project: {parser.project_name}")

    # Get models with filters
    model_list: Optional[List[str]] = list(model_names) if model_names else None
    models = parser.get_models(model_names=model_list, tag_filter=tag_filter)

    if not models:
        click.echo(
            f"{Fore.YELLOW}No models found matching the criteria.{Style.RESET_ALL}"
        )
        if tag_filter:
            click.echo(f"  Tag filter: {tag_filter}")
        if model_names:
            click.echo(f"  Model names: {', '.join(model_names)}")
        raise SystemExit(0)

    click.echo(f"{Fore.GREEN}Found {len(models)} model(s) to import:{Style.RESET_ALL}")
    for model in models:
        tags_str = f" [tags: {', '.join(model.tags)}]" if model.tags else ""
        click.echo(f"  - {model.name} ({len(model.columns)} columns){tags_str}")

    # Parse exclude columns
    excluded: Optional[List[str]] = None
    if exclude_columns:
        excluded = [c.strip() for c in exclude_columns.split(",")]

    # Create mapper
    mapper = DbtToFeastMapper(
        data_source_type=data_source_type,
        timestamp_field=timestamp_field,
        ttl_days=ttl_days,
    )

    # Generate Feast objects
    click.echo(f"\n{Fore.CYAN}Generating Feast objects...{Style.RESET_ALL}")

    all_objects: List[Any] = []
    entities_created: Dict[str, Any] = {}

    for model in models:
        # Validate timestamp field exists
        column_names = [c.name for c in model.columns]
        if timestamp_field not in column_names:
            click.echo(
                f"{Fore.YELLOW}Warning: Model '{model.name}' missing timestamp "
                f"field '{timestamp_field}'. Skipping.{Style.RESET_ALL}"
            )
            continue

        # Validate entity column exists
        if entity_column not in column_names:
            click.echo(
                f"{Fore.YELLOW}Warning: Model '{model.name}' missing entity "
                f"column '{entity_column}'. Skipping.{Style.RESET_ALL}"
            )
            continue

        # Create or reuse entity
        if entity_column not in entities_created:
            entity = mapper.create_entity(
                name=entity_column,
                description="Entity key for dbt models",
            )
            entities_created[entity_column] = entity
            all_objects.append(entity)
        else:
            entity = entities_created[entity_column]

        # Create data source
        data_source = mapper.create_data_source(
            model=model,
            timestamp_field=timestamp_field,
        )
        all_objects.append(data_source)

        # Create feature view
        feature_view = mapper.create_feature_view(
            model=model,
            source=data_source,
            entity_column=entity_column,
            entity=entity,
            timestamp_field=timestamp_field,
            ttl_days=ttl_days,
            exclude_columns=excluded,
        )
        all_objects.append(feature_view)

        click.echo(
            f"  {Fore.GREEN}✓{Style.RESET_ALL} {model.name}: "
            f"DataSource + FeatureView ({len(feature_view.features)} features)"
        )

    if not all_objects:
        click.echo(
            f"{Fore.YELLOW}No valid models to import (check warnings above).{Style.RESET_ALL}"
        )
        raise SystemExit(0)

    # Filter models that were actually processed (have valid columns)
    valid_models = [
        m
        for m in models
        if timestamp_field in [c.name for c in m.columns]
        and entity_column in [c.name for c in m.columns]
    ]

    # Summary
    click.echo(f"\n{Fore.CYAN}Summary:{Style.RESET_ALL}")
    click.echo(f"  Entities: {len(entities_created)}")
    click.echo(f"  DataSources: {len(valid_models)}")
    click.echo(f"  FeatureViews: {len(valid_models)}")

    # Generate Python file if --output specified
    if output:
        from feast.dbt.codegen import generate_feast_code

        code = generate_feast_code(
            models=valid_models,
            entity_column=entity_column,
            data_source_type=data_source_type,
            timestamp_field=timestamp_field,
            ttl_days=ttl_days,
            manifest_path=manifest_path,
            project_name=parser.project_name or "",
            exclude_columns=excluded,
            online=True,
        )

        with open(output, "w") as f:
            f.write(code)

        click.echo(
            f"\n{Fore.GREEN}✓ Generated Feast definitions: {output}{Style.RESET_ALL}"
        )
        click.echo("  You can now import this file in your feature_store.yaml repo.")
        return

    if dry_run:
        click.echo(f"\n{Fore.YELLOW}Dry run - no changes applied.{Style.RESET_ALL}")
        click.echo("Remove --dry-run flag to apply changes.")
        return

    # Apply to Feast
    click.echo(f"\n{Fore.CYAN}Applying to Feast registry...{Style.RESET_ALL}")

    repo = ctx.obj["CHDIR"]
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    store = create_feature_store(ctx)

    store.apply(all_objects)

    click.echo(
        f"{Fore.GREEN}✓ Successfully imported {len(valid_models)} dbt model(s) "
        f"to Feast project '{store.project}'{Style.RESET_ALL}"
    )


@dbt_cmd.command("list")
@click.option(
    "--manifest-path",
    "-m",
    required=True,
    type=click.Path(exists=True),
    help="Path to dbt manifest.json file",
)
@click.option(
    "--tag",
    "tag_filter",
    default=None,
    help="Filter models by dbt tag",
)
@click.option(
    "--show-columns",
    is_flag=True,
    default=False,
    help="Show column details for each model",
)
def list_command(
    manifest_path: str,
    tag_filter: Optional[str],
    show_columns: bool,
):
    """
    List dbt models available for import.

    Examples:

        # List all models
        feast dbt list -m target/manifest.json

        # List models with specific tag
        feast dbt list -m target/manifest.json --tag feast

        # Show column details
        feast dbt list -m target/manifest.json --show-columns
    """
    from feast.dbt.parser import DbtManifestParser

    click.echo(f"{Fore.CYAN}Parsing dbt manifest: {manifest_path}{Style.RESET_ALL}")

    try:
        parser = DbtManifestParser(manifest_path)
        parser.parse()
    except (FileNotFoundError, ValueError) as e:
        click.echo(f"{Fore.RED}Error: {e}{Style.RESET_ALL}", err=True)
        raise SystemExit(1)

    if parser.dbt_version:
        click.echo(f"  dbt version: {parser.dbt_version}")
    if parser.project_name:
        click.echo(f"  Project: {parser.project_name}")

    models = parser.get_models(tag_filter=tag_filter)

    if not models:
        click.echo(f"{Fore.YELLOW}No models found.{Style.RESET_ALL}")
        return

    click.echo(f"\n{Fore.GREEN}Found {len(models)} model(s):{Style.RESET_ALL}\n")

    for model in models:
        tags_str = f" [tags: {', '.join(model.tags)}]" if model.tags else ""
        click.echo(f"{Fore.CYAN}{model.name}{Style.RESET_ALL}{tags_str}")
        click.echo(f"  Table: {model.full_table_name}")
        if model.description:
            click.echo(f"  Description: {model.description[:80]}...")

        if show_columns and model.columns:
            click.echo(f"  Columns ({len(model.columns)}):")
            for col in model.columns:
                type_str = col.data_type or "unknown"
                click.echo(f"    - {col.name}: {type_str}")

        click.echo()
