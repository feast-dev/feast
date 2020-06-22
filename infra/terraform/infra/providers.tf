#Set the terraform required version

terraform {
  required_version = ">= 0.12.6"

  required_providers {
    azurerm = "= 2.13.0"
  }
}

provider "azurerm" {
  features {}
}
