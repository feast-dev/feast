FROM ubuntu:18.04

ARG REVISION
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y curl unzip locales software-properties-common && \
    apt-add-repository ppa:git-core/ppa && \
    apt update && apt install -y git

# Install Java (by default openjdk-11)
RUN apt-get install -y default-jdk

RUN locale-gen en_US.UTF-8 && update-locale LANG=en_US.utf8
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'

# Install maven
ARG MAVEN_VERSION=3.6.3
ARG SHA=c35a1803a6e70a126e80b2b3ae33eed961f83ed74d18fcd16909b2d44d7dada3203f1ffe726c17ef8dcca2dcaa9fca676987befeadc9b9f759967a8cb77181c0
ARG BASE_URL=https://apache.osuosl.org/maven/maven-3/${MAVEN_VERSION}/binaries

RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
  && curl -fsSL -o /tmp/apache-maven.tar.gz ${BASE_URL}/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
  && echo "${SHA}  /tmp/apache-maven.tar.gz" | sha512sum -c - \
  && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
  && rm -f /tmp/apache-maven.tar.gz \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ENV MAVEN_HOME /usr/share/maven

# Install Make and Python
ENV PYTHON_VERSION 3.7

RUN apt-get install -y build-essential curl python${PYTHON_VERSION} \
    python${PYTHON_VERSION}-dev python${PYTHON_VERSION}-distutils && \
    update-alternatives --install /usr/bin/python python /usr/bin/python${PYTHON_VERSION} 1 && \
    update-alternatives --set python /usr/bin/python${PYTHON_VERSION} && \
    curl -s https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python get-pip.py --force-reinstall && \
    rm get-pip.py

# Install Google Cloud SDK
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
    | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  \
    add - && apt-get update -y && apt-get install google-cloud-sdk -y

# Instal boto3
RUN pip install boto3==1.16.10

# Install Go
ENV GOLANG_VERSION 1.14.1

RUN curl -O https://storage.googleapis.com/golang/go${GOLANG_VERSION}.linux-amd64.tar.gz && \
    tar -xvf go${GOLANG_VERSION}.linux-amd64.tar.gz && chown -R root:root ./go && mv go /usr/local

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH
ENV PATH="$HOME/bin:${PATH}"

# Install Protoc and Plugins
ENV PROTOC_VERSION 3.12.2

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

# Install AZ CLI
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# Install kubectl
RUN apt-get install -y kubectl=1.20.4-00

# Install helm
RUN curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 && \
    chmod 700 get_helm.sh && \
    ./get_helm.sh --version v3.4.2

# Install jq
RUN apt-get install -y jq
