# Feast Test Infrastructure

This folder contains the Feast test infrastructure.

## Components

* test-image/ - Base docker image and script for running tests.
* prow/ - Prow configuration (plugins and jobs)
* tf/ - Terraform modules to provision the base testing infrastructure on GCP

## Set up

These steps will allow you to provision the test infrastructure in an empty GCP project. It is not necessary to rerun these steps once the infrastructure has been provisioned.

1. Make sure you have access to your GCP project and that you Terraform installed.

2. Create a bucket for maintaining Terraform state in your GCP project. This can be done manually. Update all `backend.tf` files to point to this bucket.

3. Import the bucket to manage it's own state

```
mv tf/gcs
terraform import google_storage_bucket.kf-feast-terraform-state kf-feast-terraform-state
```

4. Ensure that all variables are set correctly in `tf/terraform.tfvars`. It is likely that the GCP project will need to be updated.

5. Create the primary Kubernetes cluster which will host Prow and Argo

```
mv ../k8s-cluster
terraform apply -var-file="../terraform.tfvars"
```

6. Create a ci-bot account on GitHub with owner access to your organization. Create and save a personal access token for the bot.

7. Install and run Tackle

```
go get -u k8s.io/test-infra/prow/cmd/tackle
tackle
```

Follow the steps to install Prow in your cluster. If any step fails, follow this [guide](https://github.com/kubernetes/test-infra/blob/master/prow/getting_started_deploy.md) to complete the steps manually. 

Prow should now be receiving events from the Feast repository. You should be able to access Prow on an Ingress IP that the cluster exposes.

## Updating Prow Jobs

To update Prow jobs, plugins, or the Docker image used for testing, modify one or more of the following: 

- `prow/config.yaml`
- `prow/plugins.yaml`
- `test-image/Dockerfile`
- `test-image/run.sh`

After making modifications, run `make`. This will update the Prow configuration, build a new test image, and push it to the container registry.