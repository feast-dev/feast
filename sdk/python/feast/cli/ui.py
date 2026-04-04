import click

from feast.repo_operations import create_feature_store, registry_dump

VALID_MODES = ("proto", "rest", "rest-external")


@click.command()
@click.option(
    "--host",
    "-h",
    type=click.STRING,
    default="0.0.0.0",
    show_default=True,
    help="Specify a host for the server",
)
@click.option(
    "--port",
    "-p",
    type=click.INT,
    default=8888,
    show_default=True,
    help="Specify a port for the server",
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
    "--root_path",
    help="Provide root path to make the UI working behind proxy",
    type=click.STRING,
    default="",
)
@click.option(
    "--key",
    "-k",
    "tls_key_path",
    type=click.STRING,
    default="",
    show_default=False,
    help="path to TLS(SSL) certificate private key. You need to pass --cert arg as well to start server in TLS mode",
)
@click.option(
    "--cert",
    "-c",
    "tls_cert_path",
    type=click.STRING,
    default="",
    show_default=False,
    help="path to TLS(SSL) certificate public key. You need to pass --key arg as well to start server in TLS mode",
)
@click.option(
    "--mode",
    "-m",
    type=click.Choice(VALID_MODES, case_sensitive=False),
    default="proto",
    show_default=True,
    help=(
        "Data serving mode for the UI. "
        "'proto' serves the registry as a protobuf blob (current default). "
        "'rest' mounts the REST registry API alongside the UI. "
        "'rest-external' proxies to an external REST registry API."
    ),
)
@click.option(
    "--rest-api-url",
    type=click.STRING,
    default="",
    help="Base URL of an external REST registry API (required when --mode=rest-external). Example: http://registry-host:6570/api/v1",
)
@click.pass_context
def ui(
    ctx: click.Context,
    host: str,
    port: int,
    registry_ttl_sec: int,
    root_path: str = "",
    tls_key_path: str = "",
    tls_cert_path: str = "",
    mode: str = "proto",
    rest_api_url: str = "",
):
    """
    Shows the Feast UI over the current directory
    """
    if (tls_key_path and not tls_cert_path) or (not tls_key_path and tls_cert_path):
        raise click.BadParameter(
            "Please configure --key and --cert args to start the feature server in SSL mode."
        )
    if mode == "rest-external" and not rest_api_url:
        raise click.BadParameter(
            "--rest-api-url is required when using --mode=rest-external."
        )
    store = create_feature_store(ctx)
    store.serve_ui(
        host=host,
        port=port,
        get_registry_dump=registry_dump,
        registry_ttl_sec=registry_ttl_sec,
        root_path=root_path,
        tls_key_path=tls_key_path,
        tls_cert_path=tls_cert_path,
        mode=mode,
        rest_api_url=rest_api_url,
    )
