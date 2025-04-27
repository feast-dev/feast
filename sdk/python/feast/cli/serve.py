import click

from feast.constants import (
    DEFAULT_FEATURE_TRANSFORMATION_SERVER_PORT,
    DEFAULT_OFFLINE_SERVER_PORT,
    DEFAULT_REGISTRY_SERVER_PORT,
)
from feast.repo_operations import create_feature_store


@click.command("serve")
@click.option(
    "--host",
    "-h",
    type=click.STRING,
    default="127.0.0.1",
    show_default=True,
    help="Specify a host for the server",
)
@click.option(
    "--port",
    "-p",
    type=click.INT,
    default=6566,
    show_default=True,
    help="Specify a port for the server",
)
@click.option(
    "--type",
    "-t",
    "type_",
    type=click.STRING,
    default="http",
    show_default=True,
    help="Specify a server type: 'http' or 'grpc'",
)
@click.option(
    "--no-access-log",
    is_flag=True,
    show_default=True,
    help="Disable the Uvicorn access log",
)
@click.option(
    "--workers",
    "-w",
    type=click.INT,
    default=1,
    show_default=True,
    help="Number of worker",
)
@click.option(
    "--keep-alive-timeout",
    type=click.INT,
    default=5,
    show_default=True,
    help="Timeout for keep alive",
)
@click.option(
    "--registry_ttl_sec",
    "-r",
    help="Number of seconds after which the registry is refreshed",
    type=click.INT,
    default=5,
    show_default=True,
)
@click.option(
    "--key",
    "-k",
    "tls_key_path",
    type=click.STRING,
    default="",
    show_default=False,
    help="path to TLS certificate private key. You need to pass --cert as well to start server in TLS mode",
)
@click.option(
    "--cert",
    "-c",
    "tls_cert_path",
    type=click.STRING,
    default="",
    show_default=False,
    help="path to TLS certificate public key. You need to pass --key as well to start server in TLS mode",
)
@click.option(
    "--metrics",
    "-m",
    is_flag=True,
    show_default=True,
    help="Enable the Metrics Server",
)
@click.pass_context
def serve_command(
    ctx: click.Context,
    host: str,
    port: int,
    type_: str,
    no_access_log: bool,
    workers: int,
    metrics: bool,
    keep_alive_timeout: int,
    tls_key_path: str,
    tls_cert_path: str,
    registry_ttl_sec: int = 5,
):
    """Start a feature server locally on a given port."""
    if (tls_key_path and not tls_cert_path) or (not tls_key_path and tls_cert_path):
        raise click.BadParameter(
            "Please pass --cert and --key args to start the feature server in TLS mode."
        )
    store = create_feature_store(ctx)

    store.serve(
        host=host,
        port=port,
        type_=type_,
        no_access_log=no_access_log,
        workers=workers,
        metrics=metrics,
        keep_alive_timeout=keep_alive_timeout,
        tls_key_path=tls_key_path,
        tls_cert_path=tls_cert_path,
        registry_ttl_sec=registry_ttl_sec,
    )


@click.command("serve_transformations")
@click.option(
    "--port",
    "-p",
    type=click.INT,
    default=DEFAULT_FEATURE_TRANSFORMATION_SERVER_PORT,
    help="Specify a port for the server",
)
@click.pass_context
def serve_transformations_command(ctx: click.Context, port: int):
    """[Experimental] Start a feature consumption server locally on a given port."""
    store = create_feature_store(ctx)

    store.serve_transformations(port)


@click.command("serve_registry")
@click.option(
    "--port",
    "-p",
    type=click.INT,
    default=DEFAULT_REGISTRY_SERVER_PORT,
    help="Specify a port for the server",
)
@click.option(
    "--key",
    "-k",
    "tls_key_path",
    type=click.STRING,
    default="",
    show_default=False,
    help="path to TLS certificate private key. You need to pass --cert as well to start server in TLS mode",
)
@click.option(
    "--cert",
    "-c",
    "tls_cert_path",
    type=click.STRING,
    default="",
    show_default=False,
    help="path to TLS certificate public key. You need to pass --key as well to start server in TLS mode",
)
@click.pass_context
def serve_registry_command(
    ctx: click.Context,
    port: int,
    tls_key_path: str,
    tls_cert_path: str,
):
    """Start a registry server locally on a given port."""
    if (tls_key_path and not tls_cert_path) or (not tls_key_path and tls_cert_path):
        raise click.BadParameter(
            "Please pass --cert and --key args to start the registry server in TLS mode."
        )
    store = create_feature_store(ctx)

    store.serve_registry(port, tls_key_path, tls_cert_path)


@click.command("serve_offline")
@click.option(
    "--host",
    "-h",
    type=click.STRING,
    default="127.0.0.1",
    show_default=True,
    help="Specify a host for the server",
)
@click.option(
    "--port",
    "-p",
    type=click.INT,
    default=DEFAULT_OFFLINE_SERVER_PORT,
    help="Specify a port for the server",
)
@click.option(
    "--key",
    "-k",
    "tls_key_path",
    type=click.STRING,
    default="",
    show_default=False,
    help="path to TLS certificate private key. You need to pass --cert as well to start server in TLS mode",
)
@click.option(
    "--cert",
    "-c",
    "tls_cert_path",
    type=click.STRING,
    default="",
    show_default=False,
    help="path to TLS certificate public key. You need to pass --key as well to start server in TLS mode",
)
@click.pass_context
def serve_offline_command(
    ctx: click.Context,
    host: str,
    port: int,
    tls_key_path: str,
    tls_cert_path: str,
):
    """Start a remote server locally on a given host, port."""
    if (tls_key_path and not tls_cert_path) or (not tls_key_path and tls_cert_path):
        raise click.BadParameter(
            "Please pass --cert and --key args to start the offline server in TLS mode."
        )
    store = create_feature_store(ctx)

    store.serve_offline(host, port, tls_key_path, tls_cert_path)
