import click

from feast.repo_operations import create_feature_store


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
    "--cors-allowed-origins",
    "cors_allowed_origins",
    type=click.STRING,
    default="",
    envvar="FEAST_UI_CORS_ALLOWED_ORIGINS",
    show_default=False,
    help=(
        "Comma-separated list of trusted origins allowed to make cross-origin "
        "(CORS) requests to the UI server, e.g. "
        "'https://feast.example.com,https://app.example.com'. May also be set "
        "via the FEAST_UI_CORS_ALLOWED_ORIGINS environment variable. By default "
        "no cross-origin requests are allowed. Avoid '*', which combined with "
        "credentials exposes the server to CVE-2024-11602."
    ),
)
@click.pass_context
def ui(
    ctx: click.Context,
    host: str,
    port: int,
    root_path: str = "",
    tls_key_path: str = "",
    tls_cert_path: str = "",
    cors_allowed_origins: str = "",
):
    """
    Shows the Feast UI over the current directory
    """
    if (tls_key_path and not tls_cert_path) or (not tls_key_path and tls_cert_path):
        raise click.BadParameter(
            "Please configure --key and --cert args to start the feature server in SSL mode."
        )
    cors_allowed_origins_list = [
        origin.strip() for origin in cors_allowed_origins.split(",") if origin.strip()
    ]
    store = create_feature_store(ctx)
    store.serve_ui(
        host=host,
        port=port,
        root_path=root_path,
        tls_key_path=tls_key_path,
        tls_cert_path=tls_cert_path,
        cors_origins=cors_allowed_origins_list,
    )
