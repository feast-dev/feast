import tempfile
from typing import Optional

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from feast.infra.utils.snowflake.snowflake_utils import parse_private_key_path

PRIVATE_KEY_PASSPHRASE = "test"


def _pem_private_key(passphrase: Optional[str]):
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=(
            serialization.BestAvailableEncryption(passphrase.encode())
            if passphrase
            else serialization.NoEncryption()
        ),
    )


@pytest.fixture
def unencrypted_private_key():
    return _pem_private_key(None)


@pytest.fixture
def encrypted_private_key():
    return _pem_private_key(PRIVATE_KEY_PASSPHRASE)


def test_parse_private_key_path_key_content_unencrypted(unencrypted_private_key):
    parse_private_key_path(
        None,
        None,
        unencrypted_private_key,
    )


def test_parse_private_key_path_key_content_encrypted(encrypted_private_key):
    parse_private_key_path(
        PRIVATE_KEY_PASSPHRASE,
        None,
        encrypted_private_key,
    )


def test_parse_private_key_path_key_path_unencrypted(unencrypted_private_key):
    with tempfile.NamedTemporaryFile(mode="wb") as f:
        f.write(unencrypted_private_key)
        f.flush()
        parse_private_key_path(
            None,
            f.name,
            None,
        )


def test_parse_private_key_path_key_path_encrypted(encrypted_private_key):
    with tempfile.NamedTemporaryFile(mode="wb") as f:
        f.write(encrypted_private_key)
        f.flush()
        parse_private_key_path(
            PRIVATE_KEY_PASSPHRASE,
            f.name,
            None,
        )
