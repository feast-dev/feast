import logging
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def configure_ca_trust_store_env_variables():
    """
    configures the environment variable so that other libraries or servers refer to the TLS ca file path.
    :param ca_file_path:
    :return:
    """
    if (
        "FEAST_CA_CERT_FILE_PATH" in os.environ
        and os.environ["FEAST_CA_CERT_FILE_PATH"]
    ):
        logger.info(
            f"Feast CA Cert file path found in environment variable FEAST_CA_CERT_FILE_PATH={os.environ['FEAST_CA_CERT_FILE_PATH']}. Going to refer this path."
        )
        os.environ["SSL_CERT_FILE"] = os.environ["FEAST_CA_CERT_FILE_PATH"]
        os.environ["REQUESTS_CA_BUNDLE"] = os.environ["FEAST_CA_CERT_FILE_PATH"]
