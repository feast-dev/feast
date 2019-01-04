resource "google_storage_bucket" "kf-feast-terraform-state" {
  name     = "kf-feast-terraform-state"
  location = "US"
}