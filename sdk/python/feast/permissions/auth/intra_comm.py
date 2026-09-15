import logging
import os
from typing import Any, Optional

import jwt

logger = logging.getLogger(__name__)

INTRA_COMMUNICATION_ENV_VAR = "INTRA_COMMUNICATION_BASE64"
INTRA_COMMUNICATION_ALGORITHM = "HS256"


def get_intra_comm_secret() -> Optional[str]:
    """
    Return the shared secret used to authenticate Feast intra-server communication.

    Returns:
        Optional[str]: the configured secret, or `None` when intra-server communication
            is not configured, which covers both an unset and an empty environment
            variable. Callers must treat `None` as "intra-server communication is
            disabled" and never fall back to a default, since a well-known value would
            let any caller assume the internal identity.
    """
    return os.getenv(INTRA_COMMUNICATION_ENV_VAR) or None


def encode_intra_comm_token(claims: dict[str, Any], secret: str) -> str:
    """
    Sign an intra-server communication token with the shared secret.

    Args:
        claims: the claims identifying the internal caller.
        secret: the shared secret, used as the signing key.

    Returns:
        str: the signed token.
    """
    return jwt.encode(claims, secret, algorithm=INTRA_COMMUNICATION_ALGORITHM)


def decode_intra_comm_token(access_token: str, secret: str) -> Optional[dict[str, Any]]:
    """
    Return the claims of an intra-server communication token whose signature verifies.

    A token that is not signed with the shared secret is not an intra-server
    communication token. Ordinary user tokens issued by the identity provider reach
    this function too and must fall through to the regular authentication path, so an
    unverifiable token is reported as `None` rather than raised.

    Args:
        access_token: the raw bearer token.
        secret: the shared secret, used as the verification key.

    Returns:
        Optional[dict[str, Any]]: the verified claims, or `None` when the token is not a
            valid intra-server communication token.
    """
    try:
        return jwt.decode(
            access_token,
            secret,
            algorithms=[INTRA_COMMUNICATION_ALGORITHM],
            options={"verify_aud": False},
        )
    except jwt.InvalidTokenError:
        return None
