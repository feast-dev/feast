FROM python:3.9-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME

# Copy app handler code
COPY sdk/python/feast/infra/feature_servers/gcp_cloudrun/app.py ./app.py

# Copy necessary parts of the Feast codebase
COPY sdk/python ./sdk/python
COPY protos ./protos
COPY README.md ./README.md

# Install production dependencies.
RUN pip install --no-cache-dir \
    -e 'sdk/python[gcp,redis]' \
    -r ./sdk/python/feast/infra/feature_servers/gcp_cloudrun/requirements.txt

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
CMD exec gunicorn -k uvicorn.workers.UvicornWorker --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app
