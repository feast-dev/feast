import imp
import os
from setuptools import find_packages, setup, Command

NAME = 'Feast'
DESCRIPTION = 'Python sdk for Feast'
URL = 'https://github.com/gojek/feast'
EMAIL = 'zhiling.c@go-jek.com'
AUTHOR = 'Feast'
REQUIRES_PYTHON = '>=3.6.0'
VERSION = imp.load_source(
    'feast.version', os.path.join('feast', 'version.py')).VERSION
REQUIRED = [
    'google-api-core>=1.7.0',
    'google-auth>=1.6.0',
    'google-cloud-bigquery>=1.8.0',
    'google-cloud-storage>=1.13.0',
    'googleapis-common-protos>=1.5.5',
    'grpcio>=1.16.1',
    'jinja2',
    'pandas',
    'protobuf>=3.0.0',
    'PyYAML',
]

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=('tests',)),
    install_requires=REQUIRED,
    include_package_data=True,
    license='Apache',
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ]
)