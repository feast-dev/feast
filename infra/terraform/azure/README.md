# Terraform config for Feast on Azure

This serves as a guide on how to deploy Feast on Azure. At the end of this guide, we will have provisioned:
1. AKS cluster
2. Feast services running on AKS
3. Azure Cache (Redis) as online store
4. Spark operator on AKS
5. Kafka running on AKS.

# Steps

1. Create a tfvars file, e.g. `my.tfvars`. A sample configuration is as below:

```
name_prefix             = "feast-0-9"
resource_group          = "Feast" # pre-exisiting resource group
aks_namespace           = "default"
```

3. Configure tf state backend, e.g.:
```
terraform {
  backend "azurerm" {
    storage_account_name = "<your storage account name>"
    container_name       = "<your container name>"
    key                  = "<your blob name>"
  }
}
```

3. Use `terraform apply -var-file="my.tfvars"` to deploy.
