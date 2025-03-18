FROM registry.access.redhat.com/ubi8/python-311:1

USER 0
RUN yum install -y ninja-build
