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
            for fv, fn, dt in feature_list
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
                        "Source": json.loads(str(getattr(fv, "batch_source", "N/A"))),
                    }
                )
    if not feature_details:
        click.echo(f"Feature '{feature_name}' not found in any feature view.")
        return

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
    store = create_feature_store(ctx)
    entity_dict: dict[str, List[str]] = {}
    for entity in entities:
        try:
            key, value = entity.split("=")
            if key not in entity_dict:
                entity_dict[key] = []
            entity_dict[key].append(value)
        except ValueError:
            click.echo(f"Invalid entity format: {entity}. Use key=value format.")
            return
    entity_rows = [
        dict(zip(entity_dict.keys(), values)) for values in zip(*entity_dict.values())
    ]
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
    store = create_feature_store(ctx)
    if not dataframe and not start_date and not end_date:
        click.echo(
            "Either --dataframe or --start-date and/or --end-date must be provided."
        )
        return

    if dataframe and (start_date or end_date):
        click.echo("Cannot specify both --dataframe and --start-date/--end-date.")
        return

    entity_df = None
    if dataframe:
        try:
            entity_list = json.loads(dataframe)
            if not isinstance(entity_list, list):
                raise ValueError("Entities must be a list of dictionaries.")

            entity_df = pd.DataFrame(entity_list)
            entity_df["event_timestamp"] = pd.to_datetime(entity_df["event_timestamp"])

        except Exception as e:
            click.echo(f"Error parsing entities JSON: {e}", err=True)
            return

    feature_vector = store.get_historical_features(
        entity_df=entity_df,
        features=list(features),
        start_date=datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        if start_date
        else None,
        end_date=datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S") if end_date else None,
    ).to_df()

    click.echo(feature_vector.to_json(orient="records", indent=4))
