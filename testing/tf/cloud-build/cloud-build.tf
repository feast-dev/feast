variable "base64tagRegex" {
  default = "XigwfFsxLTldXFxkKilcXC4oMHxbMS05XVxcZCopXFwuKDB8WzEtOV1cXGQqKSg/Oi0oKD86MHxbMS05XVxcZCp8XFxkKlthLXpBLVotXVswLTlhLXpBLVotXSopKD86XFwuKD86MHxbMS05XVxcZCp8XFxkKlthLXpBLVotXVswLTlhLXpBLVotXSopKSopKT8oPzpcXCsoWzAtOWEtekEtWi1dKyg/OlxcLlswLTlhLXpBLVotXSspKikpPyQ="
}

resource "google_cloudbuild_trigger" "build_trigger" {
  project  = "${var.gcp_project}"
  trigger_template {
    branch_name = "master"
    tag_name = "${base64decode(var.base64tagRegex)}"
    project     = "${var.gcp_project}"
    repo_name   = "http://github.com/gojek/feast"
  }
  filename = "testing/tf/cloud-build/cloudbuild.yaml"
}