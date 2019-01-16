resource "google_cloudbuild_trigger" "build_trigger" {
  project  = "${var.gcp_project}"
  trigger_template {
    tag_name = "${base64decode("XigwfFsxLTldXFxkKilcXC4oMHxbMS05XVxcZCopXFwuKDB8WzEtOV1cXGQqKSg/Oi0oKD86MHxbMS05XVxcZCp8XFxkKlthLXpBLVotXVswLTlhLXpBLVotXSopKD86XFwuKD86MHxbMS05XVxcZCp8XFxkKlthLXpBLVotXVswLTlhLXpBLVotXSopKSopKT8oPzpcXCsoWzAtOWEtekEtWi1dKyg/OlxcLlswLTlhLXpBLVotXSspKikpPyQ=")}"
    project     = "${var.gcp_project}"
    repo_name   = "http://github.com/gojek/feast"
  }
  filename = "testing/tf/cloud-build/cloudbuild.yaml"
}
