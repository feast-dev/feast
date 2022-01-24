FROM python:3.7

WORKDIR /usr/src/

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN git clone https://github.com/feast-dev/feast.git /root/feast
RUN cd /root/feast/sdk/python && pip install -e '.[redis]'

WORKDIR /app
COPY . .
EXPOSE 8080

CMD ["/bin/sh", "-c", "python materialize.py && feast serve_transformations --port 8080"]
