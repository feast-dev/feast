resource "google_cloudbuild_trigger" "build_trigger" {
  project  = "${var.gcp_project}"
  trigger_template {
    branch_name = "master"
    tag_name = "^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
    project     = "${var.gcp_project}"
    repo_name   = "http://github.com/gojek/feast"
  }
  filename = "testing/tf/cloud-build/cloudbuild.yaml"
}