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

.PHONY: test-integration wait-job

ifeq ($(TYPE), PR)
test-integration:
	$(shell argo submit tests/redis-bq-dataflow/workflow.yaml -p prId=$(ID))
	$(eval job_name := $(shell argo list -o=name | head -n 1))
	wait-job job_name=$(job_name)
else
test-integration:
	$(shell argo submit tests/redis-bq-dataflow/workflow.yaml -p gitRef=$(ID))
	$(eval job_name := $(shell argo list -o=name | head -n 1))
	wait-job job_name=$(job_name)
endif

wait-job:
	job_status=$$(argo get $(job_name) | grep "Status" | awk -F ' ' '{print $$2}'); \
	while [ "$${job_status}" == "Running" ]; do \
		echo "$(job_name): $${job_status}"; \
		argo wait $(job_name); \
		job_status=$(argo get $(job_name) | grep "Status" | awk -F ' ' '{print $2}'); \
	done; \
	if [ "$${job_status}" != "Succeeded" ]; then \
		echo integration test failed. Check logs for job name $(job_name) for more details.; \
		exit 1; \
	fi