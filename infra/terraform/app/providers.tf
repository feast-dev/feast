#Set the terraform required version

terraform {
  required_version = ">= 0.12.6"

  required_providers {
    azurerm    = "= 2.13.0"
    databricks = "= 0.2.0"
    helm       = "~> 1.2.2"
    kubernetes = "~> 1.11.3"
  }
}

provider "azurerm" {
  features {}
}

provider "databricks" {
  azure_auth = {
    managed_resource_group = var.databricks_managed_resource_group_name
    azure_region           = var.databricks_location
    workspace_name         = var.databricks_name
    resource_group         = var.databricks_resource_group_name
  }
}
