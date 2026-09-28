from datetime import datetime, timedelta, timezone
from typing import Any

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

ISSUER = "https://idp.example.com/oauth2/default"
AUDIENCE = "api://registry"
CLIENT_ID = "placeholder"


def generate_rsa_keypair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


def default_claims(**overrides: Any) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    claims: dict[str, Any] = {
        "sub": "okta-app",
        "iss": ISSUER,
        "aud": AUDIENCE,
        "exp": now + timedelta(hours=1),
        "iat": now,
    }
    claims.update(overrides)
    return {key: value for key, value in claims.items() if value is not None}


def encode_jwt(private_key, **overrides: Any) -> str:
    return jwt.encode(default_claims(**overrides), private_key, algorithm="RS256")


def patch_oidc_jwks(monkeypatch, public_key) -> None:
    monkeypatch.setattr(
        "feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data",
        lambda self, *args, **kwargs: {
            "authorization_endpoint": "https://example/oauth2/v1/authorize",
            "token_endpoint": "https://example/oauth2/v1/token",
            "jwks_uri": "https://example/oauth2/v1/keys",
        },
    )

    class _SigningKey:
        def __init__(self, key):
            self.key = key

    signing_key = _SigningKey(public_key)
    monkeypatch.setattr(
        "feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt",
        lambda self, access_token: signing_key,
    )
