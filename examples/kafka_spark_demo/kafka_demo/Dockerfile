FROM python:3.8-slim

# Copy app handler code
COPY kafka_demo/kafka_demo.py kafka_demo.py
COPY feature_repo/data/driver_stats.parquet .

# Install dependencies
RUN pip3 install kafka-python==2.0.2
RUN pip3 install pandas
RUN pip3 install pyarrow

CMD [ "python", "-u", "kafka_demo.py", "--mode", "setup", "--bootstrap_servers", "broker:29092"]
