import grpc


def get_auth_metadata_plugin():
    """
    Get an Authentication Metadata Plugin. This plugin is used in gRPC to
    sign requests. Please see the following URL for more details
    https://grpc.github.io/grpc/python/_modules/grpc.html#AuthMetadataPlugin

    New plugins can be added to this function. For the time being we only
    support Google Open ID authentication.

    Returns: Returns an implementation of grpc.AuthMetadataPlugin
    """
    return GoogleOpenIDAuthMetadataPlugin()


class GoogleOpenIDAuthMetadataPlugin(grpc.AuthMetadataPlugin):
    """A `gRPC AuthMetadataPlugin`_ that inserts the credentials into each
    request.

    .. _gRPC AuthMetadataPlugin:
        http://www.grpc.io/grpc/python/grpc.html#grpc.AuthMetadataPlugin
    """

    def __init__(self):
        super(GoogleOpenIDAuthMetadataPlugin, self).__init__()
        from google.auth.transport import requests

        self._request = requests.Request()
        self._token = None
        self._refresh_token()

    def get_signed_meta(self):
        """ Creates a signed authorization metadata token."""
        return (("authorization", "Bearer {}".format(self._token)),)

    def _refresh_token(self):
        """ Refreshes Google ID token and persists it in memory """
        from google import auth as google_auth

        credentials, _ = google_auth.default()
        credentials.refresh(self._request)
        self._token = credentials.id_token

    def __call__(self, context, callback):
        """Passes authorization metadata into the given callback.

        Args:
            context (grpc.AuthMetadataContext): The RPC context.
            callback (grpc.AuthMetadataPluginCallback): The callback that will
                be invoked to pass in the authorization metadata.
        """
        callback(self.get_signed_meta(), None)
