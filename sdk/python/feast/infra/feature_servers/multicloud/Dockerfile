FROM registry.access.redhat.com/ubi9/python-312-minimal:1

USER 0
RUN microdnf install -y gcc libpq-devel python3.12-devel && microdnf clean all
USER 1001

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
ENV UV_CACHE_DIR=/tmp/uv-cache
COPY requirements.txt requirements.txt
RUN uv pip install -r requirements.txt

# modify permissions to support running with a random uid
RUN chmod g+w $(python -c "import feast.ui as ui; print(ui.__path__)" | tr -d "[']")/build/projects-list.json
