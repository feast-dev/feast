FROM public.ecr.aws/lambda/python:3.9

RUN yum install -y git


# Copy app handler code
COPY sdk/python/feast/infra/materialization/lambda/app.py ${LAMBDA_TASK_ROOT}

# Copy necessary parts of the Feast codebase
COPY sdk/python sdk/python
COPY protos protos
COPY go go
COPY setup.py setup.py
COPY pyproject.toml pyproject.toml
COPY README.md README.md

# Install Feast for AWS with Lambda dependencies
# We need this mount thingy because setuptools_scm needs access to the
# git dir to infer the version of feast we're installing.
# https://github.com/pypa/setuptools_scm#usage-from-docker
# I think it also assumes that this dockerfile is being built from the root of the directory.
RUN --mount=source=.git,target=.git,type=bind pip3 install --no-cache-dir -e '.[aws,redis]'

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.handler" ]
