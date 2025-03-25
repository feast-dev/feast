import ipaddress
import logging
import os
import shutil
from datetime import datetime, timedelta

import certifi
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import dh, dsa, ec, rsa
from cryptography.x509 import load_pem_x509_certificate
from cryptography.x509.oid import NameOID

logger = logging.getLogger(__name__)


def generate_self_signed_cert(
    cert_path="cert.pem", key_path="key.pem", common_name="localhost"
):
    """
    Generate a self-signed certificate and save it to the specified paths.

    :param cert_path: Path to save the certificate (PEM format)
    :param key_path: Path to save the private key (PEM format)
    :param common_name: Common name (CN) for the certificate, defaults to 'localhost'
    """
    # Generate private key
    key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )

    # Create a self-signed certificate
    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "California"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Feast"),
            x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        ]
    )

    # Define the certificate's Subject Alternative Names (SANs)
    alt_names = [
        x509.DNSName("localhost"),  # Hostname
        x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),  # Localhost IP
        x509.IPAddress(ipaddress.IPv4Address("0.0.0.0")),  # Bind-all IP (optional)
    ]
    san = x509.SubjectAlternativeName(alt_names)

    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.utcnow())
        .not_valid_after(
            # Certificate valid for 1 year
            datetime.utcnow() + timedelta(days=365)
        )
        .add_extension(san, critical=False)
        .sign(key, hashes.SHA256(), default_backend())
    )

    # Write the private key to a file
    with open(key_path, "wb") as f:
        f.write(
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )

    # Write the certificate to a file
    with open(cert_path, "wb") as f:
        f.write(certificate.public_bytes(serialization.Encoding.PEM))

    logger.info(
        f"Self-signed certificate and private key have been generated at {cert_path} and {key_path}."
    )


def create_ca_trust_store(
    public_key_path: str, private_key_path: str, output_trust_store_path: str
):
    """
    Create a new CA trust store as a copy of the existing one (if available),
    and add the provided public certificate to it.

    :param public_key_path: Path to the public certificate (e.g., PEM file).
    :param private_key_path: Path to the private key (optional, to verify signing authority).
    :param output_trust_store_path: Path to save the new trust store.
    """
    try:
        # Step 1: Identify the existing trust store (if available via environment variables)
        existing_trust_store = os.environ.get("SSL_CERT_FILE") or os.environ.get(
            "REQUESTS_CA_BUNDLE"
        )

        # Step 2: Copy the existing trust store to the new location (if it exists)
        if existing_trust_store and os.path.exists(existing_trust_store):
            shutil.copy(existing_trust_store, output_trust_store_path)
            logger.info(
                f"Copied existing trust store from {existing_trust_store} to {output_trust_store_path}"
            )
        else:
            # Log the creation of a new trust store (without opening a file unnecessarily)
            logger.info(
                f"No existing trust store found. Creating a new trust store at {output_trust_store_path}"
            )

        # Step 3: Load and validate the public certificate
        with open(public_key_path, "rb") as pub_file:
            public_cert_data = pub_file.read()
            public_cert = load_pem_x509_certificate(
                public_cert_data, backend=default_backend()
            )

        # Verify the private key matches (optional, adds validation)
        if private_key_path:
            with open(private_key_path, "rb") as priv_file:
                private_key_data = priv_file.read()
                private_key = serialization.load_pem_private_key(
                    private_key_data, password=None, backend=default_backend()
                )
                private_pub = private_key.public_key()
                cert_pub = public_cert.public_key()

                if isinstance(
                    private_pub,
                    (
                        rsa.RSAPublicKey,
                        dsa.DSAPublicKey,
                        ec.EllipticCurvePublicKey,
                        dh.DHPublicKey,
                    ),
                ) and isinstance(
                    cert_pub,
                    (
                        rsa.RSAPublicKey,
                        dsa.DSAPublicKey,
                        ec.EllipticCurvePublicKey,
                        dh.DHPublicKey,
                    ),
                ):
                    if private_pub.public_numbers() != cert_pub.public_numbers():
                        raise ValueError(
                            "Public certificate does not match the private key."
                        )
                else:
                    logger.warning(
                        "Key type does not support public_numbers(). Skipping strict public key match."
                    )

        # Step 4: Add the public certificate to the new trust store
        with open(output_trust_store_path, "ab") as trust_store_file:
            trust_store_file.write(public_cert.public_bytes(serialization.Encoding.PEM))

        logger.info(
            f"Trust store created/updated successfully at: {output_trust_store_path}"
        )

    except Exception as e:
        logger.error(f"Error creating CA trust store: {e}")


def combine_trust_stores(custom_cert_path: str, output_combined_path: str):
    """
    Combine the default certifi CA bundle with a custom certificate file.

    :param custom_cert_path: Path to the custom certificate PEM file.
    :param output_combined_path: Path where the combined CA bundle will be saved.
    """
    try:
        # Get the default certifi CA bundle
        certifi_ca_bundle = certifi.where()

        with open(output_combined_path, "wb") as combined_file:
            # Write the default CA bundle
            with open(certifi_ca_bundle, "rb") as default_file:
                combined_file.write(default_file.read())

            # Append the custom certificates
            with open(custom_cert_path, "rb") as custom_file:
                combined_file.write(custom_file.read())

        logger.info(f"Combined trust store created at: {output_combined_path}")

    except Exception as e:
        logger.error(f"Error combining trust stores: {e}")
        raise e
