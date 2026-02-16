1. Install Docker and [uv](https://docs.astral.sh/uv/getting-started/installation/)
2. Create a virtual environment and activate it:
```bash
uv venv --python 3.11
source .venv/bin/activate
```
3. Install dependencies:
```bash
make install-python-dependencies-dev
```
4. Start the Docker daemon
5. Run unit tests:
```bash
make test-python-unit
```
