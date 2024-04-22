FROM python:3.9

WORKDIR /usr/src/

COPY java/serving/src/test/resources/docker-compose/feast10/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY java/serving/src/test/resources/docker-compose/feast10/ .
COPY sdk/python /mnt/feast/sdk/python
COPY protos /mnt/feast/protos
COPY setup.py /mnt/feast/setup.py
COPY README.md /mnt/feast/README.md
RUN cd /mnt/feast && SETUPTOOLS_SCM_PRETEND_VERSION="0.1.0" pip install .[grpcio,redis]
EXPOSE 8080

CMD ["./entrypoint.sh"]
