from setuptools import setup
from setuptools import find_packages

NAME = "Python Redis Connector"
DESCRIPTION = "Python Redis Connector Example"
URL = "https://github.com/feast-dev/feast/go/test_repo"
AUTHOR = "Feast"
REQUIRES_PYTHON = ">=3.7.0"

REQUIRED = [
    "grpcio",
    "grpcio-health-checking",
    "mmh3",
    "redis",
    "protobuf"
]

setup(
    name=NAME,
    author=AUTHOR,
    description=DESCRIPTION,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=["connector_python"],
    install_requires=REQUIRED,
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    entry_points={"console_scripts": ["plugin=test_repo.plugin:serve"]},
)