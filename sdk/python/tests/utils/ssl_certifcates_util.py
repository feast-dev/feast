import ipaddress
import logging
from datetime import datetime, timedelta

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509 import load_pem_x509_certificate
from cryptography.hazmat.backends import default_backend

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


def create_ca_trust_store(public_key_path:str, private_key_path:str, output_trust_store_path:str):
    """
    Create a CA trust store file and add the public certificate to it.

    :param public_key_path: Path to the public certificate (e.g., PEM file).
    :param private_key_path: Path to the private key (optional, to verify signing authority).
    :param output_trust_store_path: Path to save the trust store.
    """
    try:
        # Read and load the public certificate
        with open(public_key_path, "rb") as pub_file:
            public_cert_data = pub_file.read()
            public_cert = load_pem_x509_certificate(public_cert_data, backend=default_backend())

        # Verify the private key matches (optional, adds validation)
        if private_key_path:
            with open(private_key_path, "rb") as priv_file:
                private_key_data = priv_file.read()
                private_key = serialization.load_pem_private_key(
                    private_key_data, password=None, backend=default_backend()
                )
                # Check the public/private key match
                if private_key.public_key().public_numbers() != public_cert.public_key().public_numbers():
                    raise ValueError("Public certificate does not match the private key.")

        # Create or append to the trust store
        with open(output_trust_store_path, "ab") as trust_store_file:
            trust_store_file.write(public_cert.public_bytes(serialization.Encoding.PEM))

        print(f"Trust store created/updated successfully at: {output_trust_store_path}")

    except Exception as e:
        print(f"Error creating CA trust store: {e}")
