#!/usr/bin/env bash

python -m pip install --upgrade pip setuptools wheel
make install-python
python -m pip install -qr tests/requirements.txt

pytest tests/integration/