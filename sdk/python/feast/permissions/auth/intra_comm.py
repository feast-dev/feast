"""Signed tokens for server-to-server calls between Feast components.

A Feast server that calls another one (for example a feature server reading
from a remote registry server) sends a token signed with the shared secret in
the ``INTRA_COMMUNICATION_BASE64`` environment variable. The receiving server
trusts the token only when its signature checks out against that same secret,
so a caller that does not hold the secret cannot forge one.
"""

import os
from typing import Optional

import jwt

INTRA_COMM_ALGORITHM = "HS256"


def get_intra_comm_secret() -> Optional[str]:
    """Return the shared secret, or ``None`` when it is unset or empty."""
    return os.getenv("INTRA_COMMUNICATION_BASE64") or None


def encode_intra_comm_token(payload: dict, secret: str) -> str:
    """Sign *payload* with the shared secret."""
    return jwt.encode(payload, secret, algorithm=INTRA_COMM_ALGORITHM)


def decode_intra_comm_token(access_token: str) -> Optional[dict]:
    """Return the claims of *access_token* if it is signed with the shared
    secret, or ``None`` if the secret is not configured or the token was not
    signed with it."""
    secret = get_intra_comm_secret()
    if secret is None:
        return None
    try:
        return jwt.decode(access_token, secret, algorithms=[INTRA_COMM_ALGORITHM])
    except jwt.PyJWTError:
        return None
