provider "google" {
  version     = "~> 1.20"
  # credentials = "${file("~/.secrets/${local.project_name}.json")}"
  project     = "${local.project_name}"
}

provider "helm" {
  kubernetes {
    host  = "${module.cluster.endpoint}"
    token = "${data.google_client_config.current.access_token}"

    client_certificate     = "${base64decode(module.cluster.client_certificate)}"
    client_key             = "${base64decode(module.cluster.client_key)}"
    cluster_ca_certificate = "${base64decode(module.cluster.cluster_ca_certificate)}"
  }
}
