FROM hashicorp/terraform:light

RUN apk add --update jq wget python

RUN wget -qO- https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-233.0.0-linux-x86_64.tar.gz | tar xzf - \
  && /google-cloud-sdk/bin/gcloud -q components install kubectl \
  && /google-cloud-sdk/bin/gcloud -q components install beta

RUN wget -qO- https://storage.googleapis.com/kubernetes-helm/helm-v2.12.3-linux-arm64.tar.gz | tar xzf - \
  && mv linux-arm64/helm /bin/helm

RUN helm init --client-only
