FROM registry.access.redhat.com/ubi9/python-312-minimal:1

USER 0
RUN microdnf install -y git gcc make libpq-devel python3.12-devel tar gzip && microdnf clean all
# python3.12-devel upgrades python3.12-libs to a version whose _sqlite3
# extension requires sqlite3_deserialize — absent from UBI9's sqlite-libs
# (3.34.1). Install a compatible SQLite to provide the missing symbol.
RUN curl -sSL https://www.sqlite.org/2025/sqlite-autoconf-3490200.tar.gz | tar xz && \
    cd sqlite-autoconf-3490200 && \
    ./configure --prefix=/usr --libdir=/usr/lib64 && \
    make -j$(nproc) && make install && \
    rm -f /usr/lib64/libsqlite3.so.0.8.6 && \
    ldconfig && cd .. && rm -rf sqlite-autoconf-3490200
USER 1001

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
ENV UV_CACHE_DIR=/tmp/uv-cache
COPY requirements.txt requirements.txt
RUN uv pip install -r requirements.txt

# modify permissions to support running with a random uid
RUN chmod g+w $(python -c "import feast.ui as ui; print(ui.__path__)" | tr -d "[']")/build/projects-list.json
