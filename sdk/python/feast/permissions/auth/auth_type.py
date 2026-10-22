import enum


class AuthType(enum.Enum):
    """
    Identify the type of authorization.
    """

    NONE = "no_auth"
    OIDC = "oidc"
    KUBERNETES = "kubernetes"
