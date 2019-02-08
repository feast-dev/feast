FROM debian:stretch

RUN apt-get update && apt-get install -y jq wget python zip

RUN wget -qO- https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-233.0.0-linux-x86_64.tar.gz | tar xzf - \
  && /google-cloud-sdk/bin/gcloud -q components install kubectl \
  && /google-cloud-sdk/bin/gcloud -q components install beta

RUN wget https://releases.hashicorp.com/terraform/0.11.11/terraform_0.11.11_linux_amd64.zip \
  && unzip terraform_0.11.11_linux_amd64.zip \
  && mv terraform /bin/terraform

RUN wget -qO- https://storage.googleapis.com/kubernetes-helm/helm-v2.12.3-linux-arm64.tar.gz | tar xzf - \
  && mv linux-arm64/helm /bin/helm

RUN helm init --client-only
