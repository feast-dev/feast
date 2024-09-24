# Build the manager binary
FROM quay.io/operator-framework/helm-operator:v1.34.1

ENV HOME=/opt/helm
COPY watches.yaml ${HOME}/watches.yaml
COPY --from=helmcharts feast-feature-server ${HOME}/helm-charts/feast-feature-server
WORKDIR ${HOME}
