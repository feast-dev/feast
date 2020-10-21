FROM python:3.7-slim-buster

USER root
WORKDIR /feast

COPY sdk/python sdk/python
COPY Makefile Makefile
COPY protos protos

# Install make
RUN apt-get update && apt-get -y install make git

# Install Python dependencies
RUN make compile-protos-python

# Install Feast SDK
COPY .git .git
COPY README.md README.md
RUN pip install -e sdk/python -U

CMD ["feast", "server"]
