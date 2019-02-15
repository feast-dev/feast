 #!/bin/bash

 # Copyright 2018 Google LLC
 #
 # Licensed under the Apache License, Version 2.0 (the "License");
 # you may not use this file except in compliance with the License.
 # You may obtain a copy of the License at
 #
 #      http://www.apache.org/licenses/LICENSE-2.0
 #
 # Unless required by applicable law or agreed to in writing, software
 # distributed under the License is distributed on an "AS IS" BASIS,
 # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 # See the License for the specific language governing permissions and
 # limitations under the License.


# from https://github.com/terraform-google-modules/terraform-google-kubernetes-engine/blob/176ad6f47d2d92ac9a49ef112830da2f539b737d/scripts/wait-for-cluster.sh
set -e

PROJECT=$1
CLUSTER_NAME=$2
gcloud_command="gcloud container clusters list --project=$PROJECT --format=json"
jq_query=".[] | select(.name==\"$CLUSTER_NAME\") | .status"

echo "Waiting for cluster $2 in project $1 to reconcile..."

current_status=$($gcloud_command | jq -r "$jq_query")

while [ "${current_status}" = "RECONCILING" ]; do
    printf "."
    sleep 5
    current_status=$($gcloud_command | jq -r "$jq_query")
done

echo "Cluster is ready!"