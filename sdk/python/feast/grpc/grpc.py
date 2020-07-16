import grpc


def create_grpc_channel(
    url: str,
    enable_ssl: bool = False,
    enable_auth: bool = False,
    ssl_server_cert_path: str = None,
    auth_metadata_plugin: grpc.AuthMetadataPlugin = None,
    timeout: int = 3,
) -> grpc.Channel:
    """
    Create a gRPC channel
    Args:
        url: gRPC URL to connect to
        enable_ssl: Enable TLS/SSL, optionally provide a server side certificate
        enable_auth: Enable user auth
        ssl_server_cert_path: (optional) Path to certificate (used with
            "enable SSL")
        auth_metadata_plugin: Metadata plugin to use to sign requests, only used
            with "enable auth" when SSL/TLS is enabled
        timeout: Connection timeout to server

    Returns: Returns a grpc.Channel
    """
    if not url:
        raise ValueError("Unable to create gRPC channel. URL has not been defined.")

    if enable_ssl or url.endswith(":443"):
        # User has provided a public key certificate
        if ssl_server_cert_path:
            with open(ssl_server_cert_path, "rb",) as f:
                credentials = grpc.ssl_channel_credentials(f.read())
        # Guess the certificate location
        else:
            credentials = grpc.ssl_channel_credentials()

        # Authentication is enabled, add the metadata plugin in order to sign
        # requests
        if enable_auth:
            credentials = grpc.composite_channel_credentials(
                credentials, grpc.metadata_call_credentials(auth_metadata_plugin),
            )
        channel = grpc.secure_channel(url, credentials=credentials)
    else:
        channel = grpc.insecure_channel(url)
    try:
        grpc.channel_ready_future(channel).result(timeout=timeout)
        return channel
    except grpc.FutureTimeoutError:
        raise ConnectionError(
            f"Connection timed out while attempting to connect to {url}"
        )
