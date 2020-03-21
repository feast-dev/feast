FROM maven:3.6-jdk-11

ENV PYTHON_VERSION 3.7
ENV GOLANG_VERSION 1.14.1

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
    | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  \
    add - && apt-get update -y && apt-get install google-cloud-sdk -y

# Update dependencies
RUN apt-get update

# Install Make and Python
RUN apt-get install -y build-essential curl python${PYTHON_VERSION} \
    python${PYTHON_VERSION}-dev python${PYTHON_VERSION}-distutils && \
    update-alternatives --install /usr/bin/python python /usr/bin/python${PYTHON_VERSION} 1 && \
    update-alternatives --set python /usr/bin/python${PYTHON_VERSION} && \
    curl -s https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python get-pip.py --force-reinstall && \
    rm get-pip.py


# Install Go
RUN curl -O https://storage.googleapis.com/golang/go${GOLANG_VERSION}.linux-amd64.tar.gz && \
    tar -xvf go${GOLANG_VERSION}.linux-amd64.tar.gz && chown -R root:root ./go && mv go /usr/local
ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

# Add contents of local Feast repository to image (execute from Feast root)
COPY . /feast/

# Install all dependencies
RUN cd /feast && make install-ci-dependencies