FROM python:3.9

WORKDIR /usr/src/

COPY java/serving/src/test/resources/docker-compose/feast10/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app
COPY java/serving/src/test/resources/docker-compose/feast10/ .
COPY . /mnt/feast
RUN cd /mnt/feast && pip install .[grpcio,redis]
EXPOSE 8080

CMD ["./entrypoint.sh"]
