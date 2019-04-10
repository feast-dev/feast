FROM debian:stretch

RUN apt-get update && apt-get install -y make curl gnupg wget python git

# Install kubectl gcloud and argo
RUN wget -qO- https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-241.0.0-linux-x86_64.tar.gz | tar xzf - \
    && /google-cloud-sdk/bin/gcloud -q components install kubectl \
    && /google-cloud-sdk/install.sh \
    && ln -s /google-cloud-sdk/bin/gcloud /usr/bin/gcloud
RUN curl -sSL -o /usr/local/bin/argo https://github.com/argoproj/argo/releases/download/v2.2.1/argo-linux-amd64 \
    && chmod +x /usr/local/bin/argo

COPY run.sh /usr/local/bin/

ENTRYPOINT ["run.sh"]