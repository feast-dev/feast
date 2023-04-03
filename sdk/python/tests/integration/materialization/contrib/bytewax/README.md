# Running Bytewax integration tests

To run the Bytewax integration tests, you'll need to provision a cluster using [eksctl.](https://docs.aws.amazon.com/eks/latest/userguide/eksctl.html).

## Creating an EKS cluster

In this directory is a configuration file for a single-node EKS cluster

To create the EKS cluster needed for testing, issue the following command:

``` shell
> eksctl create cluster -f ./eks-config.yaml
```

When the tests are complete, delete the created cluster with:

``` shell
> eksctl delete cluster bytewax-feast-cluster
```



