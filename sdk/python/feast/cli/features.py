import json
from datetime import datetime
from typing import List

import click
import pandas as pd

from feast.repo_operations import create_feature_store


@click.group(name="features")
def features_cmd():
    """
    Access features
    """
    pass


@features_cmd.command(name="list")
@click.option(
    "--output",
    type=click.Choice(["table", "json"], case_sensitive=False),
    default="table",
    show_default=True,
    help="Output format",
)
@click.pass_context
def features_list(ctx: click.Context, output: str):
    """
    List all features
    """
    store = create_feature_store(ctx)
    feature_views = [
        *store.list_batch_feature_views(),
        *store.list_on_demand_feature_views(),
        *store.list_stream_feature_views(),
    ]
    feature_list = []
    for fv in feature_views:
        for feature in fv.features:
            feature_list.append([feature.name, fv.name, str(feature.dtype)])

    if output == "json":
        json_output = [
            {"feature_name": fn, "feature_view": fv, "dtype": dt}
            for fn, fv, dt in feature_list
        ]
        click.echo(json.dumps(json_output, indent=4))
    else:
        from tabulate import tabulate

        click.echo(
            tabulate(
                feature_list,
                headers=["Feature", "Feature View", "Data Type"],
                tablefmt="plain",
            )
        )


@features_cmd.command("describe")
@click.argument("feature_name", type=str)
@click.pass_context
def describe_feature(ctx: click.Context, feature_name: str):
    """
    Describe a specific feature by name
    """
    store = create_feature_store(ctx)
    feature_views = [
        *store.list_batch_feature_views(),
        *store.list_on_demand_feature_views(),
        *store.list_stream_feature_views(),
    ]

    feature_details = []
    for fv in feature_views:
        for feature in fv.features:
            if feature.name == feature_name:
                feature_details.append(
                    {
                        "Feature Name": feature.name,
                        "Feature View": fv.name,
                        "Data Type": str(feature.dtype),
                        "Description": getattr(feature, "description", "N/A"),
                        "Online Store": getattr(fv, "online", "N/A"),
                        "Source": json.loads(str(getattr(fv, "batch_source", None)))
                        if getattr(fv, "batch_source", None) is not None
                        else None,
                    }
                )
    if not feature_details:
        raise click.ClickException(
            f"Feature '{feature_name}' not found in any feature view."
        )

    click.echo(json.dumps(feature_details, indent=4))


@click.command("get-online-features")
@click.option(
    "--entities",
    "-e",
    type=str,
    multiple=True,
    required=True,
    help="Entity key-value pairs (e.g., driver_id=1001)",
)
@click.option(
    "--features",
    "-f",
    multiple=True,
    required=True,
    help="Features to retrieve. (e.g.,feature-view:feature-name) ex: driver_hourly_stats:conv_rate",
)
@click.pass_context
def get_online_features(ctx: click.Context, entities: List[str], features: List[str]):
    """
    Fetch online feature values for a given entity ID
    """
    entity_dict: dict[str, List[str]] = {}
    for entity in entities:
        try:
            key, value = entity.split("=", 1)
            if not key or not value:
                raise ValueError("Empty entity key or value")
            if key not in entity_dict:
                entity_dict[key] = []
            entity_dict[key].append(value)
        except ValueError as e:
            raise click.UsageError(
                "Invalid entity format. Use key=value format."
            ) from e
    if len({len(values) for values in entity_dict.values()}) > 1:
        raise click.UsageError("Each entity key must have the same number of values.")
    entity_rows = [
        dict(zip(entity_dict.keys(), values)) for values in zip(*entity_dict.values())
    ]
    store = create_feature_store(ctx)
    feature_vector = store.get_online_features(
        features=list(features),
        entity_rows=entity_rows,
    ).to_dict()

    click.echo(json.dumps(feature_vector, indent=4))


@click.command(name="get-historical-features")
@click.option(
    "--dataframe",
    "-d",
    type=str,
    help='JSON string containing entities and timestamps. Example: \'[{"event_timestamp": "2025-03-29T12:00:00", "driver_id": 1001}]\'',
)
@click.option(
    "--features",
    "-f",
    multiple=True,
    help="Features to retrieve. feature-view:feature-name ex: driver_hourly_stats:conv_rate",
)
@click.option(
    "--start-date",
    "-s",
    type=str,
    help="Start date for historical feature retrieval. Format: YYYY-MM-DD HH:MM:SS",
)
@click.option(
    "--end-date",
    "-e",
    type=str,
    help="End date for historical feature retrieval. Format: YYYY-MM-DD HH:MM:SS",
)
@click.pass_context
def get_historical_features(
    ctx: click.Context,
    dataframe: str,
    features: List[str],
    start_date: str,
    end_date: str,
):
    """
    Fetch historical feature values for a given entity ID
    """
    if not dataframe and not start_date and not end_date:
        raise click.UsageError(
            "Either --dataframe or --start-date and/or --end-date must be provided."
        )

    if dataframe and (start_date or end_date):
        raise click.UsageError(
            "Cannot specify both --dataframe and --start-date/--end-date."
        )
    if not features:
        raise click.UsageError("At least one --features value is required.")

    entity_df = None
    if dataframe:
        try:
            entity_list = json.loads(dataframe)
            if not isinstance(entity_list, list):
                raise ValueError("Entities must be a list of dictionaries.")

            entity_df = pd.DataFrame(entity_list)
            entity_df["event_timestamp"] = pd.to_datetime(entity_df["event_timestamp"])

        except (ValueError, TypeError, KeyError) as e:
            raise click.UsageError("Invalid entities JSON or event_timestamp.") from e

    try:
        parsed_start = (
            datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S") if start_date else None
        )
        parsed_end = (
            datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S") if end_date else None
        )
    except ValueError as e:
        raise click.UsageError("Dates must use YYYY-MM-DD HH:MM:SS format.") from e
    store = create_feature_store(ctx)
    feature_vector = store.get_historical_features(
        entity_df=entity_df,
        features=list(features),
        start_date=parsed_start,
        end_date=parsed_end,
    ).to_df()

    click.echo(feature_vector.to_json(orient="records", indent=4))
