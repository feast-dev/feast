FROM python:3.7-slim

# Copy app handler code
COPY sdk/python/feast/infra/transformation_servers/app.py app.py

# Copy necessary parts of the Feast codebase
COPY sdk/python sdk/python
COPY protos protos
COPY README.md README.md

# Install dependencies
RUN pip3 install -e 'sdk/python[ci]'

# Start feature transformation server
CMD [ "python", "app.py" ]
