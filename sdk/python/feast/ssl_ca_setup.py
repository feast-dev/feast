import logging
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def configure_ssl_ca(ca_file_path: str = ""):
    """
    configures the environment variable so that other libraries or servers refer the TLS ca file path.
    :param ca_file_path:
    :return:
    """
    if ca_file_path:
        os.environ["SSL_CERT_FILE"] = ca_file_path
        os.environ["REQUESTS_CA_BUNDLE"] = ca_file_path
    elif (
        "FEAST_CA_CERT_FILE_PATH" in os.environ
        and os.environ["FEAST_CA_CERT_FILE_PATH"]
    ):
        logger.info(
            f"CA Cert file path found in environment variable FEAST_CA_CERT_FILE_PATH={os.environ['FEAST_CA_CERT_FILE_PATH']}"
        )
        os.environ["SSL_CERT_FILE"] = os.environ["FEAST_CA_CERT_FILE_PATH"]
        os.environ["REQUESTS_CA_BUNDLE"] = os.environ["FEAST_CA_CERT_FILE_PATH"]
