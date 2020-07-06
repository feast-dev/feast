variable "kafka_vnet_ip" {
  default     = "10.100.3.230"
  description = "The Internal Load Balancer IP for the Kafka broker."
  type        = string
}
variable "feast_core_vnet_ip" {
  default     = "10.100.3.220"
  description = "The Internal Load Balancer IP for Feast Core."
  type        = string
}
variable "feast_online_serving_vnet_ip" {
  default     = "10.100.3.221"
  description = "The Internal Load Balancer IP for Feast Online Serving."
  type        = string
}
variable "feast_batch_serving_vnet_ip" {
  default     = "10.100.3.222"
  description = "The Internal Load Balancer IP for Feast Batch Serving."
  type        = string
}
variable "run_number" {
  description = "A unique number for each run of the CD pipeline Used to generate a unique name for Spark JARs in DBFS and a unique folder name for Delta data in Azure Storage, to ensure isolation of successive test runs."
  type        = string
}
variable "feast_core_image_repository" {
  description = "The repository name of Feast Core docker images."
  type        = string
}
variable "feast_serving_image_repository" {
  description = "The repository name of Feast Serving docker images."
  type        = string
}
variable "feast_version" {
  description = "The repository version of Feast docker images, used both for Core and Serving images."
  type        = string
}
variable "postgresql_resource_group_name" {
  description = "The resource group of the Azure PostgreSQL server."
  type        = string
}
variable "postgresql_name" {
  description = "The resource name of the Azure PostgreSQL server."
  type        = string
}
variable "postgresql_administrator_login_password" {
  description = "The administrator password of the Azure PostgreSQL server."
  type        = string
}
variable "datalake_resource_group_name" {
  description = "The resource group of the Azure Data Lake Storage Gen2 account for storing Delta data."
  type        = string
}
variable "datalake_name" {
  description = "The resource name of the Azure Data Lake Storage Gen2 account for storing Delta data."
  type        = string
}
variable "datalake_filesystem" {
  description = "The filesystem of the Azure Data Lake Storage Gen2 account for storing Delta data."
  type        = string
}
variable "redis_hostname" {
  description = "The hostname of the Azure Redis Cache instance."
  type        = string
}
variable "redis_port" {
  description = "The port of the Azure Redis Cache instance."
  type        = number
}
variable "spark_job_jars" {
  description = "The local directory from which Spark job JARs are to be uploaded to DBFS."
  type        = string
}
variable "databricks_workspace_url" {
  description = "The Azure Databricks workspace URL."
  type        = string
}
variable "databricks_managed_resource_group_name" {
  description = "The Azure Databricks managed resource group name."
  type        = string
}
variable "databricks_location" {
  description = "The Azure Databricks workspace location."
  type        = string
}
variable "databricks_resource_group_name" {
  description = "The resource group of the Azure Databricks instance."
  type        = string
}
variable "databricks_name" {
  description = "The resource name of the Azure Databricks instance."
  type        = string
}
variable "pypi_user" {
  description = "The username to use for internal pypi authentication"
  type        = string
}
variable "pypi_password" {
  description = "The password to use for internal pypi authentication"
  type        = string
}