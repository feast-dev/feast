output "acr_login_server" {
  value = azurerm_container_registry.acr.login_server
}
output "acr_admin_username" {
  value = azurerm_container_registry.acr.admin_username
}
output "acr_admin_password" {
  sensitive = true
  value     = azurerm_container_registry.acr.admin_password
}
output "postgresql_name" {
  value = azurerm_postgresql_server.postgres.name
}
output "postgresql_resource_group_name" {
  value = azurerm_postgresql_server.postgres.resource_group_name
}
output "postgresql_administrator_login_password" {
  sensitive = true
  value     = azurerm_postgresql_server.postgres.administrator_login_password
}
output "datalake_resource_group_name" {
  value = azurerm_storage_account.datalakestorage.resource_group_name
}
output "datalake_name" {
  value = azurerm_storage_account.datalakestorage.name
}
output "datalake_filesystem" {
  value = azurerm_storage_data_lake_gen2_filesystem.datalake.name
}
output "redis_hostname" {
  value = azurerm_redis_cache.redis_cluster.hostname
}
output "redis_port" {
  value = azurerm_redis_cache.redis_cluster.port
}
output "databricks_workspace_url" {
  value = "https://${azurerm_databricks_workspace.databricks.workspace_url}"
}
output "databricks_managed_resource_group_name" {
  value = azurerm_databricks_workspace.databricks.managed_resource_group_name
}
output "databricks_location" {
  value = azurerm_databricks_workspace.databricks.location
}
output "databricks_name" {
  value = azurerm_databricks_workspace.databricks.name
}
output "databricks_resource_group_name" {
  value = azurerm_databricks_workspace.databricks.resource_group_name
}
output "kube_config" {
  sensitive = true
  value     = base64encode(azurerm_kubernetes_cluster.aks.kube_config_raw)
}
