FROM python:3.6-alpine as builder
WORKDIR /build/feast-sdk
ADD sdk/python ./
RUN python setup.py sdist bdist_wheel

FROM python:3.6-stretch as test-runner
RUN apt-get update -y
RUN apt-get install -y netcat
COPY --from=builder /build/feast-sdk/dist /usr/local/lib/feast
RUN pip install -f /usr/local/lib/feast feast
WORKDIR /usr/src/tests/
ADD tests/e2e ./e2e
RUN pip install -r e2e/requirements.txt
ADD infra/docker/test-runner/conftest.py ./e2e/
ADD infra/docker/test-runner/run-e2e-test.sh ./
RUN chmod +x run-e2e-test.sh
CMD ["./run-e2e-test.sh"]