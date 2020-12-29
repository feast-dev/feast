resource "azurerm_redis_cache" "main" {
  name = "${var.name_prefix}-redis"
  location = data.azurerm_resource_group.main.location
  resource_group_name = data.azurerm_resource_group.main.name
  capacity = var.redis_capacity
  family = "C"
  sku_name = "Standard"
}
