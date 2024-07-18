FROM python:3.11-slim

RUN apt-get update && apt-get install -y git

# Copy app handler code
COPY sdk/python/feast/infra/transformation_servers/app.py app.py

# Copy necessary parts of the Feast codebase
COPY sdk/python sdk/python
COPY protos protos
COPY go go
COPY setup.py setup.py
COPY pyproject.toml pyproject.toml
COPY README.md README.md


# Install dependencies
RUN --mount=source=.git,target=.git,type=bind pip3 install --no-cache-dir '.[gcp,aws]'

# Start feature transformation server
CMD [ "python", "app.py" ]
