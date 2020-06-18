FROM jupyter/minimal-notebook:619e9cc2fc07

RUN git clone https://github.com/feast-dev/feast.git && \
    make -C feast/ compile-protos-python && \
    pip install -r feast/sdk/python/requirements-ci.txt && \
    pip install -e feast/sdk/python -U && \
    rm -rf feast/

# Install startup script
COPY infra/docker/jupyter/startup.sh /etc/startup.sh
CMD ["/etc/startup.sh"]