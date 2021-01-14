provider "azurerm" {
  version = "=2.40.0"
  features {}
}

data "azurerm_kubernetes_cluster" "main" {
  name = "${var.name_prefix}-aks"
  resource_group_name = data.azurerm_resource_group.main.name
}

provider "helm" {
  version = "~> 1.3.2"
  kubernetes {
    host                   = "${data.azurerm_kubernetes_cluster.main.kube_config.0.host}"
    username               = "${data.azurerm_kubernetes_cluster.main.kube_config.0.username}"
    password               = "${data.azurerm_kubernetes_cluster.main.kube_config.0.password}"
    client_certificate     = "${base64decode(data.azurerm_kubernetes_cluster.main.kube_config.0.client_certificate)}"
    client_key             = "${base64decode(data.azurerm_kubernetes_cluster.main.kube_config.0.client_key)}"
    cluster_ca_certificate = "${base64decode(data.azurerm_kubernetes_cluster.main.kube_config.0.cluster_ca_certificate)}"
    load_config_file = false
  }
}

provider "kubernetes" {
  version = "~> 1.13.3"
  host                   = "${data.azurerm_kubernetes_cluster.main.kube_config.0.host}"
  username               = "${data.azurerm_kubernetes_cluster.main.kube_config.0.username}"
  password               = "${data.azurerm_kubernetes_cluster.main.kube_config.0.password}"
  client_certificate     = "${base64decode(data.azurerm_kubernetes_cluster.main.kube_config.0.client_certificate)}"
  client_key             = "${base64decode(data.azurerm_kubernetes_cluster.main.kube_config.0.client_key)}"
  cluster_ca_certificate = "${base64decode(data.azurerm_kubernetes_cluster.main.kube_config.0.cluster_ca_certificate)}"
  load_config_file = false
}
