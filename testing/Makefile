# 
#  Copyright 2019 The Feast Authors
# 
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
# 
#      https://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# 

PHONY: get-cluster-credentials update-config update-plugins build-push

PROJECT       				?= kf-feast
ZONE          				?= us-central1-a
CLUSTER       				?= primary-test-cluster
REGISTRY      				?= us.gcr.io
VERSION       				?= 1.0.0
PUSH     					?= docker push

get-cluster-credentials:
	gcloud container clusters get-credentials "$(CLUSTER)" --project="$(PROJECT)" --zone="$(ZONE)"

update-config: get-cluster-credentials
	kubectl create configmap config --from-file=config.yaml=prow/config.yaml --dry-run -o yaml | kubectl replace configmap config -f -

update-plugins: get-cluster-credentials
	kubectl create configmap plugins --from-file=plugins.yaml=prow/plugins.yaml --dry-run -o yaml | kubectl replace configmap plugins -f -

build-push:
	docker build test-image/ -t $(REGISTRY)/$(PROJECT)/test-image:$(VERSION)
	$(PUSH) "$(REGISTRY)/$(PROJECT)/test-image:$(VERSION)"