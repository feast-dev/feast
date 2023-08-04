# Affirm Specific README

1. Clone the repo
2. Install poetry 1.4.1
3. Set poetry env: poetry env use 3.7.13
4. Run: poetry install
5. Run: poetry shell
6. Create venv: python3 -m venv venv/
7. Source environment: source venv/bin/activate
8. Install dev dependencies: pip install -e ".[dev]"
9. Build python protos into package:
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

