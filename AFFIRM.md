# Affirm Specific README

1. Clone the repo
2. Install poetry 1.4.1
3. Install pyenv and set your python environment to 3.9.17
   - ```commandline
     brew install pyenv
     pyenv install 3.9.17
     pyenv shell 3.9.17
     ```
4. Set poetry env: `poetry env use 3.9.17`
5. Run: `poetry install`
   - If you failed to install fastavro 1.8.0, you can try running the following to install it manually
   ```commandline
   pip install setuptools wheel 'Cython<3'
   pip install --no-build-isolation fastavro==1.8.0
   ```

6. Run: `poetry shell`
7. Create venv: `python3 -m venv venv/`
8. Source environment: `source venv/bin/activate`
9. Install dev dependencies: `pip install -e ".[dev]"`
    - If you failed to install mysqlclient, you can try running the following
    ```
   brew install mysql-client pkg-config
   export PKG_CONFIG_PATH="$(brew --prefix)/opt/mysql-client/lib/pkgconfig"
   pip install mysqlclient
    ```
10. Build python protos into package:
  - # READ THIS: uncomment line "build_python_protos": BuildPythonProtosCommand in setup.py
  - make compile-protos-python
    - If you're running into issues with this step, try running the following:
```
pip install grpcio-tools==1.48.2 &&  pip install mypy-protobuf==3.1.0
```
  - # READ THIS: recomment line "build_python_protos": BuildPythonProtosCommand in setup.py
10. Install additional dependencies (Optional, Only if you want to build golang):
  - `brew install golang`
  - And run `make protos` to build golang and python protos into package

To run unit tests: make `test-python`
  - TODO: We should fix this step.

To publish to jfrog artifactory follow our docs on Confluence to configure. Otherwise run:
```
make build-ui
# READ THIS: uncomment line "build_python_protos": BuildPythonProtosCommand in setup.py
make compile-protos-python
# READ THIS: recomment line "build_python_protos": BuildPythonProtosCommand in setup.py
rm -r dist/
python setup.py sdist bdist_wheel --universal
twine upload -r artifactory dist/*
```

