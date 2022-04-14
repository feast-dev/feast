# Updating feast wheel for Flipboard

```
git clone git@github.com:Flipboard/feast.git
git rebase origin/<version-branch>
cd sdk/python
python3.8 -m venv _env
source _env/bin/activate
python3 -m pip install --upgrade pip setuptools wheel
pip install -r requirements/py3.8-requirements.txt
pip install mypy-protobuf grpcio grpcio-tools twine

vim setup.py # update the version= to <version>.post1

python setup.py sdist bdist_wheel
aws codeartifact login --tool twine --domain flpython --domain-owner 792860931134 --repository python
twine upload --non-interactive --disable-progress-bar --skip-existing --repository codeartifact dist/* --verbose
```
