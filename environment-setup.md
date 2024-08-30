1. install anaconda, install docker
2. create an environment for feast, selecting python 3.9. Activate the environment:
```bash
conda create --name feast python=3.9
conda activate feast
```
3. install dependencies:
```bash
pip install pip-tools
brew install mysql
brew install xz protobuf openssl zlib
pip install cryptography -U
conda install protobuf
conda install pymssql
pip install -e ".[dev]"
make install-python-ci-dependencies PYTHON=3.9
```
4. start the docker daemon
5. run unit tests:
```bash
make test-python-unit
```
