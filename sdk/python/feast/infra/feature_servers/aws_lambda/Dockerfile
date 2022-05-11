FROM public.ecr.aws/lambda/python:3.9

# Copy app handler code
COPY sdk/python/feast/infra/feature_servers/aws_lambda/app.py ${LAMBDA_TASK_ROOT}

# Copy necessary parts of the Feast codebase
COPY sdk/python sdk/python
COPY protos protos
COPY go go
COPY setup.py setup.py
COPY pyproject.toml pyproject.toml
COPY README.md README.md

# Install Feast for AWS with Lambda dependencies
# TODO(felixwang9817): Remove Snowflake dependencies once lazy loading of offline stores is supported.
# See https://github.com/feast-dev/feast/issues/2566 for more details.
RUN pip3 install -e '.[aws,redis,snowflake]'
RUN pip3 install -r sdk/python/feast/infra/feature_servers/aws_lambda/requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.handler" ]
