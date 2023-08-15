FROM python:3.11-slim

RUN python3 -m pip install milvus==2.2.12

# this is needed to divert logs to stdout
RUN mkdir -p /root/.milvus.io/milvus-server/2.2.12/logs/
RUN touch /root/.milvus.io/milvus-server/2.2.12/logs/milvus-stdout.log
RUN touch /root/.milvus.io/milvus-server/2.2.12/logs/milvus-stderr.log
RUN ln -sf /dev/stdout /root/.milvus.io/milvus-server/2.2.12/logs/milvus-stdout.log \
    && ln -sf /dev/stderr /root/.milvus.io/milvus-server/2.2.12/logs/milvus-stderr.log

CMD ["milvus-server"]