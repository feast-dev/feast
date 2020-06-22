resource "random_password" "random" {
  length           = 24
  special          = true
  override_special = "!@#$%&*()-_=+[]:?"
  min_upper        = 1
  min_lower        = 1
  min_numeric      = 1
  min_special      = 1
}

# resource group to hold all resources
resource "azurerm_resource_group" "feast" {
  name     = "rg-${var.project_name}"
  location = var.location
}

# azure container registry
resource "azurerm_container_registry" "acr" {
  name                = "${var.project_name}acr"
  resource_group_name = azurerm_resource_group.feast.name
  location            = azurerm_resource_group.feast.location
  sku                 = "Basic"
  admin_enabled       = true
}
resource "azurerm_role_assignment" "aks_sp_container_registry" {
  scope                = azurerm_container_registry.acr.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_kubernetes_cluster.aks.kubelet_identity[0].object_id
}

# azure postgresql server
resource "azurerm_postgresql_server" "postgres" {
  name                = "psql-${var.project_name}"
  location            = azurerm_resource_group.feast.location
  resource_group_name = azurerm_resource_group.feast.name

  sku_name = "GP_Gen5_2"

  storage_mb            = 5120
  backup_retention_days = 7

  administrator_login          = "postgresuser"
  administrator_login_password = random_password.random.result
  version                      = "11"
  ssl_enforcement_enabled      = false
}
resource "azurerm_postgresql_virtual_network_rule" "postgres" {
  name                                 = "postgresql-vnet-rule"
  resource_group_name                  = azurerm_postgresql_server.postgres.resource_group_name
  server_name                          = azurerm_postgresql_server.postgres.name
  subnet_id                            = azurerm_subnet.aks.id
  ignore_missing_vnet_service_endpoint = true
}

# azure cache for redis
resource "azurerm_subnet" "redis_cluster" {
  name                 = "subnet-${var.project_name}-redis"
  resource_group_name  = azurerm_resource_group.feast.name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = ["10.100.4.0/24"]
}
resource "azurerm_redis_cache" "redis_cluster" {
  name                = "rediscluster-${var.project_name}"
  resource_group_name = azurerm_resource_group.feast.name
  location            = azurerm_resource_group.feast.location
  capacity            = 1
  family              = "P"
  sku_name            = "Premium"
  enable_non_ssl_port = true
  minimum_tls_version = "1.2"
  subnet_id           = azurerm_subnet.redis_cluster.id
  redis_configuration {
    enable_authentication = false
  }
}

# virtual network
resource "azurerm_virtual_network" "vnet" {
  name                = "vnet-${var.project_name}"
  resource_group_name = azurerm_resource_group.feast.name
  location            = azurerm_resource_group.feast.location

  address_space = ["10.100.0.0/16"]
}

# subnets
resource "azurerm_subnet" "public" {
  name                = "subnet-${var.project_name}-dbw-public"
  resource_group_name = azurerm_resource_group.feast.name

  virtual_network_name = azurerm_virtual_network.vnet.name

  address_prefixes = ["10.100.2.0/24"]

  delegation {
    name = "subnet-${var.project_name}-public-delegation"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}
resource "azurerm_network_security_group" "public_nsg" {
  name                = "subnet-${var.project_name}-public-nsg"
  location            = azurerm_resource_group.feast.location
  resource_group_name = azurerm_resource_group.feast.name
}
resource "azurerm_subnet_network_security_group_association" "public_nsg_association" {
  subnet_id                 = azurerm_subnet.public.id
  network_security_group_id = azurerm_network_security_group.public_nsg.id
}
resource "azurerm_subnet" "private" {
  name                = "subnet-${var.project_name}-dbw-private"
  resource_group_name = azurerm_resource_group.feast.name

  virtual_network_name = azurerm_virtual_network.vnet.name

  address_prefixes = ["10.100.1.0/24"]

  delegation {
    name = "subnet-${var.project_name}-private-delegation"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}
resource "azurerm_network_security_group" "private_nsg" {
  name                = "subnet-${var.project_name}-private-nsg"
  location            = azurerm_resource_group.feast.location
  resource_group_name = azurerm_resource_group.feast.name
}
resource "azurerm_subnet_network_security_group_association" "private_nsg_association" {
  subnet_id                 = azurerm_subnet.private.id
  network_security_group_id = azurerm_network_security_group.private_nsg.id
}

# azure kubernetes service
resource "azurerm_subnet" "aks" {
  name                 = "subnet-aks"
  resource_group_name  = azurerm_resource_group.feast.name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = ["10.100.0.0/24"]
  service_endpoints    = ["Microsoft.Sql"]
}
resource "azurerm_subnet" "aks_ilb" {
  name                 = "internal-load-balancers"
  resource_group_name  = azurerm_resource_group.feast.name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = ["10.100.3.0/24"]
}
resource "azurerm_kubernetes_cluster" "aks" {
  name                = "aks-${var.project_name}"
  location            = azurerm_resource_group.feast.location
  resource_group_name = azurerm_resource_group.feast.name
  dns_prefix          = "aks-${var.project_name}"

  node_resource_group = "mc-${azurerm_resource_group.feast.name}-aks-${var.project_name}"

  default_node_pool {
    name           = "default"
    node_count     = 2
    vm_size        = "Standard_D2_v2"
    vnet_subnet_id = azurerm_subnet.aks.id
  }

  network_profile {
    network_plugin     = "azure"
    service_cidr       = "172.16.0.0/13"
    dns_service_ip     = "172.16.0.10"
    docker_bridge_cidr = "172.24.0.1/16"
  }

  addon_profile {
    kube_dashboard {
      enabled = true
    }
    oms_agent {
      enabled                    = true
      log_analytics_workspace_id = azurerm_log_analytics_workspace.loganalyticsworkspace.id
    }
  }

  identity {
    type = "SystemAssigned"
  }
}
# Subnet permission for creating ILB
resource "azurerm_role_assignment" "aks_ilb_subnet" {
  scope                = azurerm_subnet.aks_ilb.id
  role_definition_name = "Network Contributor"
  principal_id         = azurerm_kubernetes_cluster.aks.identity[0].principal_id
}
resource "azurerm_role_assignment" "aks_subnet" {
  scope                = azurerm_subnet.aks.id
  role_definition_name = "Network Contributor"
  principal_id         = azurerm_kubernetes_cluster.aks.identity[0].principal_id
}

resource "azurerm_databricks_workspace" "databricks" {
  name                = "dbw-${var.project_name}"
  resource_group_name = azurerm_resource_group.feast.name
  location            = azurerm_resource_group.feast.location
  sku                 = "standard"

  managed_resource_group_name = "databricks-${azurerm_resource_group.feast.name}"

  custom_parameters {
    private_subnet_name = azurerm_subnet.private.name
    public_subnet_name  = azurerm_subnet.public.name
    virtual_network_id  = azurerm_virtual_network.vnet.id
  }

  depends_on = [
    azurerm_subnet_network_security_group_association.private_nsg_association,
    azurerm_subnet_network_security_group_association.public_nsg_association,
  ]
}

resource "azurerm_storage_account" "datalakestorage" {
  name                     = "dls${var.project_name}"
  resource_group_name      = azurerm_resource_group.feast.name
  location                 = azurerm_resource_group.feast.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"
}

resource "azurerm_storage_data_lake_gen2_filesystem" "datalake" {
  name               = "feast"
  storage_account_id = azurerm_storage_account.datalakestorage.id
}

resource "azurerm_log_analytics_workspace" "loganalyticsworkspace" {
  name                = "log-${var.project_name}"
  resource_group_name = azurerm_resource_group.feast.name
  location            = azurerm_resource_group.feast.location
  sku                 = "PerGB2018"
  retention_in_days   = 30
}
resource "azurerm_log_analytics_solution" "loganalyticssolution" {
  solution_name         = "ContainerInsights"
  resource_group_name   = azurerm_resource_group.feast.name
  location              = azurerm_resource_group.feast.location
  workspace_resource_id = azurerm_log_analytics_workspace.loganalyticsworkspace.id
  workspace_name        = azurerm_log_analytics_workspace.loganalyticsworkspace.name

  plan {
    publisher = "Microsoft"
    product   = "OMSGallery/ContainerInsights"
  }
}
