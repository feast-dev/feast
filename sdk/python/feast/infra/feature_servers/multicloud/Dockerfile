FROM registry.access.redhat.com/ubi9/python-312:1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
ENV UV_CACHE_DIR=/tmp/uv-cache
COPY requirements.txt requirements.txt
RUN uv pip install -r requirements.txt

# modify permissions to support running with a random uid
RUN chmod g+w $(python -c "import feast.ui as ui; print(ui.__path__)" | tr -d "[']")/build/projects-list.json
