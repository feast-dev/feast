# Feast Feature Server Helm-based Operator

Leverages [operator-sdk](https://github.com/operator-framework/operator-sdk) and the [feast-feature-server helm chart](/infra/charts/feast-feature-server).

### To run against an K8S cluster -
```bash
$ make deploy

# test the operator by deploying a feature server sample CR
$ oc apply -f config/samples/charts_v1alpha1_feastfeatureserver.yaml
```
