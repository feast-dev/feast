terraform {
  backend "gcs" {
    bucket  = "kf-feast-terraform-state"
    prefix  = "tf/k8s-cluster"
  }
}