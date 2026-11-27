import click
from sqlalchemy import create_engine

from feast.infra.registry.sql import SqlRegistryConfig, metadata
from feast.repo_config import load_repo_config
from feast.repo_operations import cli_check_repo


@click.group(name="registry")
def registry_cmd() -> None:
    """
    Manage the feature registry
    """
    pass


@registry_cmd.command("create-schema")
@click.pass_context
def registry_create_schema(ctx: click.Context) -> None:
    """
    Pre-create the SQL registry schema.

    Use this when schema_mode is set to 'verify' or 'skip' so the application
    does not need DDL privileges at runtime.
    """
    repo = ctx.obj["CHDIR"]
    fs_yaml_file = ctx.obj["FS_YAML_FILE"]
    cli_check_repo(repo, fs_yaml_file)
    repo_config = load_repo_config(repo, fs_yaml_file)

    if repo_config is None:
        raise click.ClickException("Could not load feature_store.yaml")

    registry_config = repo_config.registry
    if not isinstance(registry_config, SqlRegistryConfig):
        raise click.ClickException(
            "This command only applies to SQL-based registries "
            f"(registry_type='sql'). Current type: '{registry_config.registry_type}'"
        )

    engine = create_engine(
        registry_config.path, **registry_config.sqlalchemy_config_kwargs
    )
    metadata.create_all(engine)
    engine.dispose()
    click.echo("SQL registry schema created successfully.")
