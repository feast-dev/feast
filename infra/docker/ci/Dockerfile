FROM maven:3.6-jdk-11

# Install Google Cloud SDK
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
    | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  \
    add - && apt-get update -y && apt-get install google-cloud-sdk -y

# Install Make and Python
ENV PYTHON_VERSION 3.7

RUN apt-get install -y build-essential curl python${PYTHON_VERSION} \
    python${PYTHON_VERSION}-dev python${PYTHON_VERSION}-distutils && \
    update-alternatives --install /usr/bin/python python /usr/bin/python${PYTHON_VERSION} 1 && \
    update-alternatives --set python /usr/bin/python${PYTHON_VERSION} && \
    curl -s https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python get-pip.py --force-reinstall && \
    rm get-pip.py


# Install Go
ENV GOLANG_VERSION 1.14.1

RUN curl -O https://storage.googleapis.com/golang/go${GOLANG_VERSION}.linux-amd64.tar.gz && \
    tar -xvf go${GOLANG_VERSION}.linux-amd64.tar.gz && chown -R root:root ./go && mv go /usr/local

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH
ENV PATH="$HOME/bin:${PATH}"

# Install Protoc and Plugins
ENV PROTOC_VERSION 3.10.0

RUN PROTOC_ZIP=protoc-${PROTOC_VERSION}-linux-x86_64.zip && \
    curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/$PROTOC_ZIP && \
    unzip -o $PROTOC_ZIP -d /usr/local bin/protoc && \
    unzip -o $PROTOC_ZIP -d /usr/local 'include/*' && \
    rm -f $PROTOC_ZIP && \
    go get github.com/golang/protobuf/proto && \
    go get gopkg.in/russross/blackfriday.v2 && \
	  git clone https://github.com/istio/tools/ && \
	  cd tools/cmd/protoc-gen-docs && \
	  go build && mkdir -p $HOME/bin && cp protoc-gen-docs $HOME/bin