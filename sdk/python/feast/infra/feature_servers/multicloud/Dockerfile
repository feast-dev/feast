FROM python:3.11 AS builder

RUN apt-get update && apt-get install -y git

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME

# Copy necessary parts of the Feast codebase
COPY . .

# Install production dependencies into the virtual environment
RUN python -m venv .venv && . .venv/bin/activate && pip install '.[aws,gcp,snowflake,redis,go,mysql,postgres]'

FROM python:3.11-slim AS runner

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME

# Copy the virtual environment with the production dependencies from the builder stage
COPY --from=builder $APP_HOME/.venv $APP_HOME/.venv
ENV PATH="$APP_HOME/.venv/bin:${PATH}"

# Add runtime dependencies such as libarrow
RUN apt-get update &&  \
    apt-get install -y git cmake ca-certificates lsb-release wget && \
    wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb &&\
    apt-get install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb &&\
    apt-get update &&\
    apt-get install -y libarrow-dev &&\
    apt-get clean

RUN mkdir -m 775 /.cache